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
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/detector"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/sync"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/telemetry"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

const (
	Version = "0.3.0-dev"
	AppName = "aegisedge-agent"
)

// AnomalyHandler defines a callback for processing detected anomaly signals locally.
type AnomalyHandler func(ctx context.Context, sig *types.AnomalySignal)

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

	// 4. Initialize deterministic anomaly detector (process-local lifecycle)
	det, err := detector.NewThresholdDetector(detector.ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules:   detector.DefaultRules(),
	})
	if err != nil {
		logger.Error("failed to initialize anomaly detector", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("local anomaly detector initialized (process-local lifecycle)",
		slog.String("detector_name", det.Name()),
		slog.String("detector_version", det.Version()),
		slog.Int("rules_count", len(detector.DefaultRules())),
	)

	// 5. Initialize synchronization pipeline (if enabled)
	var syncer *sync.Syncer
	if cfg.SyncEnabled {
		if cfg.NATSEnabled {
			// NATS JetStream transport
			publisher, err := sync.NewNATSPublisher(sync.NATSPublisherConfig{
				URL:            cfg.NATSURL,
				PublishTimeout: cfg.NATSPublishTimeout,
			}, logger)
			if err != nil {
				logger.Error("failed to initialize NATS publisher", slog.Any("error", err))
				os.Exit(1)
			}
			defer func() {
				logger.Info("closing NATS publisher...")
				if err := publisher.Close(); err != nil {
					logger.Error("error closing NATS publisher", slog.Any("error", err))
				}
			}()
			syncer = sync.NewSyncerWithPublisher(store, publisher, logger)
			logger.Info("synchronization transport: NATS JetStream",
				slog.String("nats_url", cfg.NATSURL),
			)
		} else {
			// HTTP transport (existing Phase 3)
			client := sync.NewHTTPClient(cfg.ControlPlaneURL, sync.WithLogger(logger))
			syncer = sync.NewSyncer(store, client, logger)
			logger.Info("synchronization transport: HTTP",
				slog.String("control_plane_url", cfg.ControlPlaneURL),
			)
		}
	}

	// If run-once mode was requested, execute single collection step and sync attempt
	if cfg.RunOnce {
		batch, _, err := runCollectionStep(ctx, gen, store, det, logger)
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

	// 6. Daemon loop with graceful signal handling
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
			if _, _, err := runCollectionStep(ctx, gen, store, det, logger); err != nil {
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

// runCollectionStep executes the core Phase 5.3 pipeline flow:
// Generate -> Validate -> Persist to SQLite WAL -> Confirm -> Run Detector -> Log/Hand off Anomaly
//
// In accordance with ADR-0009 and Phase 5.3:
// PERSIST FIRST. DETECT SECOND.
// A detector failure MUST NOT prevent successful telemetry persistence or poison synchronization.
// If SQLite persistence fails, detection is not executed and telemetry is not accepted.
func runCollectionStep(
	ctx context.Context,
	gen telemetry.Generator,
	store storage.Store,
	det detector.Detector,
	logger *slog.Logger,
	handlers ...AnomalyHandler,
) (*types.TelemetryBatch, []*types.AnomalySignal, error) {
	// Step A: Generate deterministic telemetry batch
	batch, err := gen.GenerateBatch(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry generation failed: %w", err)
	}

	logger.Info("telemetry batch generated",
		slog.String("batch_id", batch.BatchID),
		slog.String("node_id", batch.NodeID),
		slog.Int64("sequence_number", batch.SequenceNumber),
		slog.Int("metrics_count", len(batch.Metrics)),
	)

	// Step B: Explicit domain validation
	if err := batch.Validate(); err != nil {
		return nil, nil, fmt.Errorf("batch validation failed: %w", err)
	}

	// Step C: Durable persistence to SQLite WAL (PERSIST FIRST)
	if err := store.PersistBatch(ctx, batch); err != nil {
		return nil, nil, fmt.Errorf("local persistence failed: %w", err)
	}

	// Step D: Confirm successful persistence
	rec, err := store.GetBatch(ctx, batch.BatchID)
	if err != nil {
		return nil, nil, fmt.Errorf("confirming persisted batch failed: %w", err)
	}

	logger.Info("telemetry batch locally accepted and committed to WAL",
		slog.String("batch_id", rec.BatchID),
		slog.Int64("sequence_number", rec.SequenceNumber),
		slog.String("sync_status", string(rec.SyncStatus)),
		slog.Time("collected_at", rec.CollectedAt),
	)

	// Step E: Local Anomaly Detection (DETECT SECOND)
	// The detector is an observer and must never cause persisted telemetry to be discarded or marked failed.
	var detectedSignals []*types.AnomalySignal
	if det != nil {
		signals, detErr := det.DetectBatch(ctx, batch)
		if detErr != nil {
			// ERROR ISOLATION: Log warning; do not fail or discard persisted batch.
			logger.Warn("anomaly detector evaluation encountered error; persisted telemetry remains safe and durable",
				slog.String("batch_id", batch.BatchID),
				slog.Any("error", detErr),
			)
		} else {
			detectedSignals = signals
			for _, sig := range signals {
				// Structured logging conforming to ADR-0009
				logger.Warn("anomaly detected",
					slog.String("anomaly_id", sig.AnomalyID),
					slog.String("node_id", sig.NodeID),
					slog.String("metric_name", sig.MetricName),
					slog.Float64("observed_value", sig.ObservedValue),
					slog.Float64("expected_value", sig.ExpectedValue),
					slog.Float64("anomaly_score", sig.AnomalyScore),
					slog.String("detection_method", sig.DetectionMethod),
					slog.String("detector_version", sig.DetectorVersion),
					slog.String("correlation_id", sig.CorrelationID),
				)
				for _, h := range handlers {
					if h != nil {
						h(ctx, sig)
					}
				}
			}
		}
	}

	return batch, detectedSignals, nil
}
