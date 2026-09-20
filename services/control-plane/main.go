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
	"syscall"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/server"
)

const (
	Version = "0.3.0-dev"
	AppName = "aegisedge-control-plane"
)

// ServerConfig holds runtime configuration for the control plane.
type ServerConfig struct {
	Port     int
	LogLevel string
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

	sig := <-shutdownChan
	logger.Info("shutdown signal received; stopping control plane gracefully...", slog.String("signal", sig.String()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("error during graceful shutdown", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("control plane server stopped successfully")
}
