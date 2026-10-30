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
	"strconv"
	"strings"
	"syscall"
	"time"

	cpnats "github.com/Chamalka-heshi/AegisEdge/services/control-plane/nats"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/server"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/storage"
)

const (
	Version       = "0.3.0-dev"
	AppName       = "aegisedge-control-plane"
	DefaultDBPath = "data/control-plane.db"
)

// ServerConfig holds runtime configuration for the control plane.
type ServerConfig struct {
	Host              string
	Port              int
	LogLevel          string
	NATSEnabled       bool
	NATSURL           string
	AuthEnabled       bool
	SharedSecret      string
	NodeSecretsFile   string
	AdminToken        string
	MaxClockSkew      time.Duration
	TLSCertFile       string
	TLSKeyFile        string
	MaxHistoryBatches int
	DBPath            string

	// Storage Retention & Pruning
	RetentionEnabled       bool
	TelemetryRetentionDays int
	PruneInterval          time.Duration
	PruneBatchSize         int
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
	if cfg.MaxHistoryBatches <= 0 {
		if histStr := strings.TrimSpace(os.Getenv("MAX_HISTORY_BATCHES")); histStr != "" {
			if val, err := strconv.Atoi(histStr); err == nil && val > 0 {
				cfg.MaxHistoryBatches = val
			}
		}
	}
	if cfg.MaxHistoryBatches <= 0 {
		cfg.MaxHistoryBatches = server.DefaultMaxHistoryBatches
	}
	if cfg.DBPath == "" {
		if envDB := strings.TrimSpace(os.Getenv("AEGISEDGE_CP_DB_PATH")); envDB != "" {
			cfg.DBPath = envDB
		}
	}
	if cfg.DBPath == "" {
		cfg.DBPath = DefaultDBPath
	}

	// Retention configuration defaults and environment overrides
	if !cfg.RetentionEnabled {
		if retStr := strings.TrimSpace(os.Getenv("STORAGE_RETENTION_ENABLED")); retStr != "" {
			if strings.ToLower(retStr) == "true" || retStr == "1" {
				cfg.RetentionEnabled = true
			}
		}
	}
	if cfg.TelemetryRetentionDays <= 0 {
		if daysStr := strings.TrimSpace(os.Getenv("STORAGE_RETENTION_DAYS")); daysStr != "" {
			if val, err := strconv.Atoi(daysStr); err == nil && val > 0 {
				cfg.TelemetryRetentionDays = val
			}
		}
	}
	if cfg.TelemetryRetentionDays <= 0 {
		cfg.TelemetryRetentionDays = server.DefaultTelemetryRetentionDays
	}
	if cfg.PruneInterval <= 0 {
		if intvStr := strings.TrimSpace(os.Getenv("STORAGE_PRUNE_INTERVAL")); intvStr != "" {
			if d, err := time.ParseDuration(intvStr); err == nil && d > 0 {
				cfg.PruneInterval = d
			}
		}
	}
	if cfg.PruneInterval <= 0 {
		cfg.PruneInterval = server.DefaultPruneInterval
	}
	if cfg.PruneBatchSize <= 0 {
		if sizeStr := strings.TrimSpace(os.Getenv("STORAGE_PRUNE_BATCH_SIZE")); sizeStr != "" {
			if val, err := strconv.Atoi(sizeStr); err == nil && val > 0 {
				cfg.PruneBatchSize = val
			}
		}
	}
	if cfg.PruneBatchSize <= 0 {
		cfg.PruneBatchSize = server.DefaultPruneBatchSize
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
		showVersion       = flag.Bool("version", false, "Print version and exit")
		host              = flag.String("host", "127.0.0.1", "HTTP server listening host interface")
		port              = flag.Int("port", 8080, "HTTP server listening port")
		logLevel          = flag.String("log-level", "info", "Logging level (debug, info, warn, error)")
		authEnabled       = flag.Bool("auth-enabled", false, "Enable HMAC and Bearer authentication")
		sharedSecret      = flag.String("shared-secret", "", "Shared secret for edge node HMAC authentication")
		nodeSecretsFile   = flag.String("node-secrets-file", "", "Path to JSON file containing per-node secrets mapping")
		adminToken        = flag.String("admin-token", "", "Admin Bearer token for privileged control-plane endpoints")
		maxClockSkew      = flag.Duration("max-clock-skew", 5*time.Minute, "Maximum allowed clock skew for requests")
		tlsCertFile       = flag.String("tls-cert", "", "Path to TLS certificate file")
		tlsKeyFile        = flag.String("tls-key", "", "Path to TLS private key file")
		maxHistoryBatches = flag.Int("max-history-batches", server.DefaultMaxHistoryBatches, "Maximum number of recent telemetry batch payloads retained in memory")
		dbPath            = flag.String("db-path", DefaultDBPath, "Path to SQLite database file for durable idempotency tracking")
		retentionEnabled  = flag.Bool("retention-enabled", false, "Enable background telemetry payload retention and pruning")
		retentionDays     = flag.Int("retention-days", server.DefaultTelemetryRetentionDays, "Days to retain historical telemetry payloads")
		pruneInterval     = flag.Duration("prune-interval", server.DefaultPruneInterval, "Interval between background storage pruning cycles")
		pruneBatchSize    = flag.Int("prune-batch-size", server.DefaultPruneBatchSize, "Maximum payloads pruned per pass")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("%s version %s\n", AppName, Version)
		os.Exit(0)
	}

	rawFlags := ServerConfig{
		Host:                   *host,
		Port:                   *port,
		LogLevel:               *logLevel,
		AuthEnabled:            *authEnabled,
		SharedSecret:           *sharedSecret,
		NodeSecretsFile:        *nodeSecretsFile,
		AdminToken:             *adminToken,
		MaxClockSkew:           *maxClockSkew,
		TLSCertFile:            *tlsCertFile,
		TLSKeyFile:             *tlsKeyFile,
		MaxHistoryBatches:      *maxHistoryBatches,
		DBPath:                 *dbPath,
		RetentionEnabled:       *retentionEnabled,
		TelemetryRetentionDays: *retentionDays,
		PruneInterval:          *pruneInterval,
		PruneBatchSize:         *pruneBatchSize,
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

	// Initialize durable SQLite idempotency store
	store, err := storage.OpenSQLite(cfg.DBPath)
	if err != nil {
		logger.Error("failed to open sqlite database",
			slog.String("db_path", cfg.DBPath),
			slog.Any("error", err),
		)
		os.Exit(1)
	}
	defer store.Close()

	srvInstance := server.NewServer(logger,
		server.WithIdempotencyStore(store),
		server.WithMaxHistory(cfg.MaxHistoryBatches),
		server.WithPruningConfig(server.PruningConfig{
			Enabled:                cfg.RetentionEnabled,
			TelemetryRetentionDays: cfg.TelemetryRetentionDays,
			PruneInterval:          cfg.PruneInterval,
			PruneBatchSize:         cfg.PruneBatchSize,
		}),
	)
	if err := srvInstance.InitError(); err != nil {
		logger.Error("failed to rehydrate node registry from durable storage", slog.Any("error", err))
		os.Exit(1)
	}
	srvInstance.SetAuthConfig(authConfig)

	// Start background storage retention pruner if enabled
	if cfg.RetentionEnabled {
		if err := srvInstance.StartPruner(context.Background()); err != nil {
			logger.Error("failed to start storage retention pruner", slog.Any("error", err))
		}
	}

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
			slog.String("db_path", cfg.DBPath),
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
			NATSURL:          cfg.NATSURL,
			QuarantineStore:  store,
			IdempotencyStore: store,
		}, srvInstance, logger)
		if err != nil {
			logger.Error("failed to initialize NATS consumer", slog.Any("error", err))
			os.Exit(1)
		}
		srvInstance.Metrics().SetNATSExhaustedDeliveriesSource(func() float64 {
			return float64(consumer.ExhaustedDeliveries())
		})
		srvInstance.Metrics().SetNATSQuarantinedSource(func() float64 {
			return float64(consumer.Quarantined())
		})
		srvInstance.Metrics().SetNATSRecoveredSource(func() float64 {
			return float64(consumer.Recovered())
		})
		srvInstance.Metrics().SetNATSAdvisoryStreamMessagesSource(func() float64 {
			return float64(consumer.AdvisoryStreamMessages())
		})
		srvInstance.Metrics().SetNATSAdvisoryStreamBytesSource(func() float64 {
			return float64(consumer.AdvisoryStreamBytes())
		})
		srvInstance.Metrics().SetNATSAdvisoryPendingBacklogSource(func() float64 {
			return float64(consumer.AdvisoryPendingBacklog())
		})
		srvInstance.Metrics().SetNATSAdvisoryPendingAckSource(func() float64 {
			return float64(consumer.AdvisoryPendingAck())
		})
		srvInstance.Metrics().SetNATSAdvisoryFetchErrorsSource(func() float64 {
			return float64(consumer.AdvisoryFetchErrors())
		})
		srvInstance.Metrics().SetNATSAdvisoryEvictionsSource(func() float64 {
			return float64(consumer.AdvisoryEvictions())
		})
		srvInstance.Metrics().SetNATSStalledUnacknowledgedSequencesSource(func() float64 {
			return float64(consumer.StalledUnacknowledgedSequences())
		})
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

	// Immediately mark readiness as not ready to shed new incoming traffic
	srvInstance.MarkShuttingDown()

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
	// Explicitly stop and drain the background pruner before closing storage
	srvInstance.StopPruner()

	if err := store.Close(); err != nil {
		logger.Error("error closing storage", slog.Any("error", err))
	}
	logger.Info("control plane server stopped successfully")
}
