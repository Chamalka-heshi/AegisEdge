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
	Port        int
	LogLevel    string
	NATSEnabled bool
	NATSURL     string
}

func main() {
	var (
		showVersion = flag.Bool("version", false, "Print version and exit")
		port        = flag.Int("port", 8080, "HTTP server listening port")
		logLevel    = flag.String("log-level", "info", "Logging level (debug, info, warn, error)")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("%s version %s\n", AppName, Version)
		os.Exit(0)
	}

	cfg := ServerConfig{
		Port:     *port,
		LogLevel: *logLevel,
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
	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
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
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
