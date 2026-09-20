package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/config"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/sync"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/telemetry"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

const (
	Version = "0.3.0-dev"
	AppName = "aegisedge-agent"
)

func main() {
	cfg := config.LoadFromEnv()

	var (
		showVersion     = flag.Bool("version", false, "Print version and exit")
		nodeID          = flag.String("node-id", cfg.NodeID, "Unique node identifier")
		dbPath          = flag.String("db-path", cfg.DatabasePath, "Local SQLite database path")
		interval        = flag.Duration("interval", cfg.CollectionInterval, "Telemetry collection interval")
		controlPlaneURL = flag.String("control-plane-url", cfg.ControlPlaneURL, "Control plane HTTP endpoint")
		syncInterval    = flag.Duration("sync-interval", cfg.SyncInterval, "Background synchronization interval")
		syncEnabled     = flag.Bool("sync-enabled", cfg.SyncEnabled, "Enable background synchronization")
		runOnce         = flag.Bool("once", false, "Run single collection step and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("%s version %s\n", AppName, Version)
		os.Exit(0)
	}

	cfg.NodeID = *nodeID
	cfg.DatabasePath = *dbPath
	cfg.CollectionInterval = *interval
	cfg.ControlPlaneURL = *controlPlaneURL
	cfg.SyncInterval = *syncInterval
	cfg.SyncEnabled = *syncEnabled
	cfg.RunOnce = *runOnce

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("starting aegisedge agent (offline-first sync mode)",
		slog.String("app", AppName),
		slog.String("version", Version),
		slog.String("node_id", cfg.NodeID),
		slog.String("db_path", cfg.DatabasePath),
		slog.Duration("interval", cfg.CollectionInterval),
		slog.String("control_plane_url", cfg.ControlPlaneURL),
		slog.Duration("sync_interval", cfg.SyncInterval),
		slog.Bool("sync_enabled", cfg.SyncEnabled),
		slog.Bool("once", cfg.RunOnce),
	)

	// 1. Initialize SQLite WAL local store
	logger.Info("initializing local SQLite WAL storage engine...", slog.String("db_path", cfg.DatabasePath))
	store, err := storage.OpenSQLite(cfg.DatabasePath)
	if err != nil {
		logger.Error("failed to initialize local database", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() {
		logger.Info("closing local storage engine...")
		if err := store.Close(); err != nil {
			logger.Error("error closing storage engine", slog.Any("error", err))
		}
	}()

	ctx := context.Background()

	// 2. Query highest existing sequence number for this node to ensure strict monotonicity across restarts
	latestSeq, err := store.GetLatestSequenceNumber(ctx, cfg.NodeID)
	if err != nil {
		logger.Error("failed to query latest sequence number", slog.Any("error", err))
		os.Exit(1)
	}

	initialSeq := latestSeq + 1
	logger.Info("sequence initialized",
		slog.String("node_id", cfg.NodeID),
		slog.Int64("latest_persisted_sequence", latestSeq),
		slog.Int64("next_sequence", initialSeq),
	)

	// 3. Initialize deterministic telemetry generator
	gen, err := telemetry.NewSimulatedGenerator(cfg.NodeID, initialSeq)
	if err != nil {
		logger.Error("failed to initialize telemetry generator", slog.Any("error", err))
		os.Exit(1)
	}

	// 4. Initialize synchronization pipeline (if enabled)
	var syncer *sync.Syncer
	if cfg.SyncEnabled {
		client := sync.NewHTTPClient(cfg.ControlPlaneURL, sync.WithLogger(logger))
		syncer = sync.NewSyncer(store, client, logger)
	}

	// If run-once mode was requested, execute single collection step and sync attempt
	if cfg.RunOnce {
		batch, err := runCollectionStep(ctx, gen, store, logger)
		if err != nil {
			logger.Error("single-shot collection step failed", slog.Any("error", err))
			os.Exit(1)
		}
		logger.Info("single-shot step completed successfully",
			slog.String("batch_id", batch.BatchID),
			slog.Int64("sequence_number", batch.SequenceNumber),
		)

		if syncer != nil {
			logger.Info("executing single-shot synchronization step...")
			stats, err := syncer.SyncPendingBatches(ctx, 50)
			if err != nil {
				logger.Warn("single-shot synchronization encountered error (data safely buffered locally)", slog.Any("error", err))
			} else {
				logger.Info("single-shot synchronization complete",
					slog.Int("total_synced", stats.TotalSynced),
					slog.Int("total_failed", stats.TotalFailed),
				)
			}
		}
		return
	}

	// 5. Daemon loop with graceful signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	collectTicker := time.NewTicker(cfg.CollectionInterval)
	defer collectTicker.Stop()

	var syncTicker *time.Ticker
	if syncer != nil {
		syncTicker = time.NewTicker(cfg.SyncInterval)
		defer syncTicker.Stop()
	}

	logger.Info("entering autonomous telemetry collection and synchronization loop")

	for {
		select {
		case <-sigChan:
			logger.Info("shutdown signal received; terminating agent loop cleanly")
			return

		case <-collectTicker.C:
			if _, err := runCollectionStep(ctx, gen, store, logger); err != nil {
				logger.Error("telemetry collection and persistence step failed", slog.Any("error", err))
			} else if syncer != nil {
				// Proactively trigger sync after new batch is persisted
				if _, err := syncer.SyncPendingBatches(ctx, 50); err != nil {
					logger.Debug("background sync cycle encountered transient issue", slog.Any("error", err))
				}
			}

		case <-func() <-chan time.Time {
			if syncTicker != nil {
				return syncTicker.C
			}
			return nil
		}():
			if syncer != nil {
				if _, err := syncer.SyncPendingBatches(ctx, 50); err != nil {
					logger.Debug("periodic sync cycle encountered transient issue", slog.Any("error", err))
				}
			}
		}
	}
}

// runCollectionStep executes the core Phase 2 durability invariant:
// Generate -> Validate -> Persist to SQLite WAL -> Confirm -> Mark Accepted
func runCollectionStep(
	ctx context.Context,
	gen telemetry.Generator,
	store storage.Store,
	logger *slog.Logger,
) (*types.TelemetryBatch, error) {
	// Step A: Generate deterministic telemetry batch
	batch, err := gen.GenerateBatch(ctx)
	if err != nil {
		return nil, fmt.Errorf("telemetry generation failed: %w", err)
	}

	logger.Info("telemetry batch generated",
		slog.String("batch_id", batch.BatchID),
		slog.String("node_id", batch.NodeID),
		slog.Int64("sequence_number", batch.SequenceNumber),
		slog.Int("metrics_count", len(batch.Metrics)),
	)

	// Step B: Explicit domain validation
	if err := batch.Validate(); err != nil {
		return nil, fmt.Errorf("batch validation failed: %w", err)
	}

	// Step C: Durable persistence to SQLite WAL
	if err := store.PersistBatch(ctx, batch); err != nil {
		return nil, fmt.Errorf("local persistence failed: %w", err)
	}

	// Step D: Confirm successful persistence
	rec, err := store.GetBatch(ctx, batch.BatchID)
	if err != nil {
		return nil, fmt.Errorf("confirming persisted batch failed: %w", err)
	}

	logger.Info("telemetry batch locally accepted and committed to WAL",
		slog.String("batch_id", rec.BatchID),
		slog.Int64("sequence_number", rec.SequenceNumber),
		slog.String("sync_status", string(rec.SyncStatus)),
		slog.Time("collected_at", rec.CollectedAt),
	)

	return batch, nil
}
