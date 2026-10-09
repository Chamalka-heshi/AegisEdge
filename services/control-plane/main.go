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
	Host         string
	Port         int
	LogLevel     string
	NATSEnabled  bool
	NATSURL      string
	AuthEnabled  bool
	SharedSecret string
	AdminToken   string
	MaxClockSkew time.Duration
	TLSCertFile  string
	TLSKeyFile   string
}

func main() {
	var (
		showVersion  = flag.Bool("version", false, "Print version and exit")
		host         = flag.String("host", "127.0.0.1", "HTTP server listening host interface")
		port         = flag.Int("port", 8080, "HTTP server listening port")
		logLevel     = flag.String("log-level", "info", "Logging level (debug, info, warn, error)")
		authEnabled  = flag.Bool("auth-enabled", false, "Enable HMAC and Bearer authentication")
		sharedSecret = flag.String("shared-secret", "", "Shared secret for edge node HMAC authentication")
		adminToken   = flag.String("admin-token", "", "Admin Bearer token for privileged control-plane endpoints")
		maxClockSkew = flag.Duration("max-clock-skew", 5*time.Minute, "Maximum allowed clock skew for requests")
		tlsCertFile  = flag.String("tls-cert", "", "Path to TLS certificate file")
		tlsKeyFile   = flag.String("tls-key", "", "Path to TLS private key file")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("%s version %s\n", AppName, Version)
		os.Exit(0)
	}

	cfg := ServerConfig{
		Host:         *host,
		Port:         *port,
		LogLevel:     *logLevel,
		AuthEnabled:  *authEnabled,
		SharedSecret: *sharedSecret,
		AdminToken:   *adminToken,
		MaxClockSkew: *maxClockSkew,
		TLSCertFile:  *tlsCertFile,
		TLSKeyFile:   *tlsKeyFile,
	}

	// Environment variable overrides
	if hostStr := strings.TrimSpace(os.Getenv("HOST")); hostStr != "" {
		cfg.Host = hostStr
	}
	if authStr := strings.TrimSpace(os.Getenv("AUTH_ENABLED")); authStr != "" {
		if strings.ToLower(authStr) == "true" || authStr == "1" {
			cfg.AuthEnabled = true
		}
	}
	if sec := strings.TrimSpace(os.Getenv("SHARED_SECRET")); sec != "" {
		cfg.SharedSecret = sec
		cfg.AuthEnabled = true
	}
	if tok := strings.TrimSpace(os.Getenv("ADMIN_TOKEN")); tok != "" {
		cfg.AdminToken = tok
	}
	if cert := strings.TrimSpace(os.Getenv("TLS_CERT_FILE")); cert != "" {
		cfg.TLSCertFile = cert
	}
	if key := strings.TrimSpace(os.Getenv("TLS_KEY_FILE")); key != "" {
		cfg.TLSKeyFile = key
	}

	listenAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	// Environment posture check
	envStr := strings.ToLower(strings.TrimSpace(os.Getenv("ENV")))
	if envStr == "" {
		envStr = strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
	}
	isProduction := envStr == "production" || envStr == "prod"

	authConfig := server.AuthConfig{
		Enabled:          cfg.AuthEnabled,
		SharedSecret:     cfg.SharedSecret,
		AdminToken:       cfg.AdminToken,
		MaxClockSkew:     cfg.MaxClockSkew,
		ReplayProtection: true,
		TLSCertFile:      cfg.TLSCertFile,
		TLSKeyFile:       cfg.TLSKeyFile,
	}

	// Fail closed if configuration fails safety validation
	if err := authConfig.Validate(listenAddr, isProduction); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: invalid security configuration: %v\n", err)
		os.Exit(1)
	}

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
