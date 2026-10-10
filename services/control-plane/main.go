package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	cpnats "github.com/Chamalka-heshi/AegisEdge/services/control-plane/nats"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/server"
)

const (
	Version = "0.3.0-dev"
	AppName = "aegisedge-control-plane"
)

// ServerConfig holds runtime configuration for the control plane.
type ServerConfig struct {
	Host            string
	Port            int
	LogLevel        string
	NATSEnabled     bool
	NATSURL         string
	AuthEnabled     bool
	SharedSecret    string
	NodeSecretsFile string
	AdminToken      string
	MaxClockSkew    time.Duration
	TLSCertFile     string
	TLSKeyFile      string
}

// BuildServerConfig constructs and validates ServerConfig and AuthConfig from CLI flags and environment variables.
func BuildServerConfig(flags ServerConfig, isProduction bool) (ServerConfig, server.AuthConfig, error) {
	cfg := flags

	// Environment variable defaults (CLI flags take precedence if specified)
	if cfg.Host == "" || cfg.Host == "127.0.0.1" {
		if hostStr := strings.TrimSpace(os.Getenv("HOST")); hostStr != "" {
			cfg.Host = hostStr
		}
	}
	if !cfg.AuthEnabled {
		if authStr := strings.TrimSpace(os.Getenv("AUTH_ENABLED")); authStr != "" {
			if strings.ToLower(authStr) == "true" || authStr == "1" {
				cfg.AuthEnabled = true
			}
		}
	}
	if cfg.SharedSecret == "" {
		if sec := strings.TrimSpace(os.Getenv("SHARED_SECRET")); sec != "" {
			cfg.SharedSecret = sec
			cfg.AuthEnabled = true
		}
	}
	if cfg.NodeSecretsFile == "" {
		if nsf := strings.TrimSpace(os.Getenv("NODE_SECRETS_FILE")); nsf != "" {
			cfg.NodeSecretsFile = nsf
			cfg.AuthEnabled = true
		}
	}
	if cfg.AdminToken == "" {
		if tok := strings.TrimSpace(os.Getenv("ADMIN_TOKEN")); tok != "" {
			cfg.AdminToken = tok
		}
	}
	if cfg.TLSCertFile == "" {
		if cert := strings.TrimSpace(os.Getenv("TLS_CERT_FILE")); cert != "" {
			cfg.TLSCertFile = cert
		}
	}
	if cfg.TLSKeyFile == "" {
		if key := strings.TrimSpace(os.Getenv("TLS_KEY_FILE")); key != "" {
			cfg.TLSKeyFile = key
		}
	}

	listenAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	var nodeSecrets map[string]string
	if cfg.NodeSecretsFile != "" {
		var err error
		nodeSecrets, err = server.LoadNodeSecretsFile(cfg.NodeSecretsFile)
		if err != nil {
			return cfg, server.AuthConfig{}, fmt.Errorf("failed to load node secrets file: %w", err)
		}
	}

	authConfig := server.AuthConfig{
		Enabled:          cfg.AuthEnabled,
		SharedSecret:     cfg.SharedSecret,
		NodeSecrets:      nodeSecrets,
		AdminToken:       cfg.AdminToken,
		MaxClockSkew:     cfg.MaxClockSkew,
		ReplayProtection: true,
		TLSCertFile:      cfg.TLSCertFile,
		TLSKeyFile:       cfg.TLSKeyFile,
	}

	if err := authConfig.Validate(listenAddr, isProduction); err != nil {
		return cfg, authConfig, fmt.Errorf("invalid security configuration: %w", err)
	}

	return cfg, authConfig, nil
}

func main() {
	var (
		showVersion     = flag.Bool("version", false, "Print version and exit")
		host            = flag.String("host", "127.0.0.1", "HTTP server listening host interface")
		port            = flag.Int("port", 8080, "HTTP server listening port")
		logLevel        = flag.String("log-level", "info", "Logging level (debug, info, warn, error)")
		authEnabled     = flag.Bool("auth-enabled", false, "Enable HMAC and Bearer authentication")
		sharedSecret    = flag.String("shared-secret", "", "Shared secret for edge node HMAC authentication")
		nodeSecretsFile = flag.String("node-secrets-file", "", "Path to JSON file containing per-node secrets mapping")
		adminToken      = flag.String("admin-token", "", "Admin Bearer token for privileged control-plane endpoints")
		maxClockSkew    = flag.Duration("max-clock-skew", 5*time.Minute, "Maximum allowed clock skew for requests")
		tlsCertFile     = flag.String("tls-cert", "", "Path to TLS certificate file")
		tlsKeyFile      = flag.String("tls-key", "", "Path to TLS private key file")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("%s version %s\n", AppName, Version)
		os.Exit(0)
	}

	rawFlags := ServerConfig{
		Host:            *host,
		Port:            *port,
		LogLevel:        *logLevel,
		AuthEnabled:     *authEnabled,
		SharedSecret:    *sharedSecret,
		NodeSecretsFile: *nodeSecretsFile,
		AdminToken:      *adminToken,
		MaxClockSkew:    *maxClockSkew,
		TLSCertFile:     *tlsCertFile,
		TLSKeyFile:      *tlsKeyFile,
	}

	// Environment posture check
	envStr := strings.ToLower(strings.TrimSpace(os.Getenv("ENV")))
	if envStr == "" {
		envStr = strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
	}
	isProduction := envStr == "production" || envStr == "prod"

	cfg, authConfig, err := BuildServerConfig(rawFlags, isProduction)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}

	listenAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	// NATS configuration from environment
	if natsStr := strings.TrimSpace(os.Getenv("NATS_ENABLED")); natsStr != "" {
		if strings.ToLower(natsStr) == "true" || natsStr == "1" {
			cfg.NATSEnabled = true
		}
	}
	cfg.NATSURL = strings.TrimSpace(os.Getenv("NATS_URL"))
	if cfg.NATSURL == "" {
		cfg.NATSURL = "nats://127.0.0.1:4222"
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	srvInstance := server.NewServer(logger)
	srvInstance.SetAuthConfig(authConfig)

	httpServer := &http.Server{
		Addr:         listenAddr,
		Handler:      srvInstance.Routes(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		logger.Info("starting control-plane http server",
			slog.String("app", AppName),
			slog.String("version", Version),
			slog.Int("port", cfg.Port),
			slog.Bool("auth_enabled", cfg.AuthEnabled),
			slog.Bool("tls_enabled", cfg.TLSCertFile != ""),
		)
		var err error
		if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
			err = httpServer.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			err = httpServer.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server encountered fatal error", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	// Start NATS consumer if enabled
	var consumer *cpnats.TelemetryConsumer
	if cfg.NATSEnabled {
		var err error
		consumer, err = cpnats.NewTelemetryConsumer(cpnats.ConsumerConfig{
			NATSURL: cfg.NATSURL,
		}, srvInstance, logger)
		if err != nil {
			logger.Error("failed to initialize NATS consumer", slog.Any("error", err))
			os.Exit(1)
		}
		if err := consumer.Start(); err != nil {
			logger.Error("failed to start NATS consumer", slog.Any("error", err))
			os.Exit(1)
		}
		logger.Info("NATS JetStream consumer started",
			slog.String("nats_url", cfg.NATSURL),
		)
	}

	sig := <-shutdownChan
	logger.Info("shutdown signal received; stopping control plane gracefully...", slog.String("signal", sig.String()))

	// Stop NATS consumer first
	if consumer != nil {
		consumer.Stop()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("error during graceful shutdown", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("control plane server stopped successfully")
}
