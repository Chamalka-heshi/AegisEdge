package config

import (
	"os"
	"strings"
	"time"
)

const (
	DefaultNodeID                   = "edge-node-01"
	DefaultDatabasePath             = "data/aegisedge-edge.db"
	DefaultCollectionInterval       = 5 * time.Second
	DefaultControlPlaneURL          = "http://localhost:8080"
	DefaultSyncInterval             = 5 * time.Second
	DefaultNATSURL                  = "nats://127.0.0.1:4222"
	DefaultNATSPublishTimeout       = 5 * time.Second
	DefaultModelStoreDir            = "data/models"
	DefaultMetricsEnabled           = false
	DefaultMetricsAddr              = "127.0.0.1:9091"
	DefaultShutdownTimeout          = 10 * time.Second
	DefaultCoordinationEnabled      = true
	DefaultHeartbeatInterval        = 10 * time.Second
	DefaultInitialReconnectInterval = 1 * time.Second
	DefaultMaxReconnectInterval     = 30 * time.Second
)

// Config represents runtime configuration parameters for the edge agent.
type Config struct {
	NodeID                   string
	DatabasePath             string
	CollectionInterval       time.Duration
	ControlPlaneURL          string
	SyncInterval             time.Duration
	SyncEnabled              bool
	RunOnce                  bool
	NATSEnabled              bool
	NATSURL                  string
	NATSPublishTimeout       time.Duration
	ModelStoreDir            string
	MetricsEnabled           bool
	MetricsAddr              string
	ShutdownTimeout          time.Duration
	CoordinationEnabled      bool
	HeartbeatInterval        time.Duration
	InitialReconnectInterval time.Duration
	MaxReconnectInterval     time.Duration
}

// LoadFromEnv loads configuration from environment variables, falling back to sensible defaults.
func LoadFromEnv() Config {
	nodeID := strings.TrimSpace(os.Getenv("AEGISEDGE_NODE_ID"))
	if nodeID == "" {
		nodeID = DefaultNodeID
	}

	dbPath := strings.TrimSpace(os.Getenv("AEGISEDGE_DATABASE_PATH"))
	if dbPath == "" {
		dbPath = DefaultDatabasePath
	}

	interval := DefaultCollectionInterval
	if intervalStr := strings.TrimSpace(os.Getenv("AEGISEDGE_COLLECTION_INTERVAL")); intervalStr != "" {
		if parsed, err := time.ParseDuration(intervalStr); err == nil && parsed > 0 {
			interval = parsed
		}
	}

	controlPlaneURL := strings.TrimSpace(os.Getenv("AEGISEDGE_CONTROL_PLANE_URL"))
	if controlPlaneURL == "" {
		controlPlaneURL = DefaultControlPlaneURL
	}

	syncInterval := DefaultSyncInterval
	if syncIntervalStr := strings.TrimSpace(os.Getenv("AEGISEDGE_SYNC_INTERVAL")); syncIntervalStr != "" {
		if parsed, err := time.ParseDuration(syncIntervalStr); err == nil && parsed > 0 {
			syncInterval = parsed
		}
	}

	syncEnabled := true
	if syncEnabledStr := strings.TrimSpace(os.Getenv("AEGISEDGE_SYNC_ENABLED")); syncEnabledStr != "" {
		if strings.ToLower(syncEnabledStr) == "false" || syncEnabledStr == "0" {
			syncEnabled = false
		}
	}

	natsEnabled := false
	if natsEnabledStr := strings.TrimSpace(os.Getenv("NATS_ENABLED")); natsEnabledStr != "" {
		if strings.ToLower(natsEnabledStr) == "true" || natsEnabledStr == "1" {
			natsEnabled = true
		}
	}

	natsURL := strings.TrimSpace(os.Getenv("NATS_URL"))
	if natsURL == "" {
		natsURL = DefaultNATSURL
	}

	natsPublishTimeout := DefaultNATSPublishTimeout
	if natsTimeoutStr := strings.TrimSpace(os.Getenv("NATS_PUBLISH_TIMEOUT")); natsTimeoutStr != "" {
		if parsed, err := time.ParseDuration(natsTimeoutStr); err == nil && parsed > 0 {
			natsPublishTimeout = parsed
		}
	}

	modelStoreDir := strings.TrimSpace(os.Getenv("AEGISEDGE_MODEL_STORE_DIR"))
	if modelStoreDir == "" {
		modelStoreDir = DefaultModelStoreDir
	}

	metricsEnabled := DefaultMetricsEnabled
	if metricsEnabledStr := strings.TrimSpace(os.Getenv("METRICS_ENABLED")); metricsEnabledStr != "" {
		if strings.ToLower(metricsEnabledStr) == "true" || metricsEnabledStr == "1" {
			metricsEnabled = true
		}
	}

	metricsAddr := strings.TrimSpace(os.Getenv("METRICS_ADDR"))
	if metricsAddr == "" {
		metricsAddr = DefaultMetricsAddr
	}

	shutdownTimeout := DefaultShutdownTimeout
	if shutdownTimeoutStr := strings.TrimSpace(os.Getenv("AEGISEDGE_SHUTDOWN_TIMEOUT")); shutdownTimeoutStr != "" {
		if parsed, err := time.ParseDuration(shutdownTimeoutStr); err == nil && parsed > 0 {
			shutdownTimeout = parsed
		}
	}

	coordinationEnabled := DefaultCoordinationEnabled
	if coordStr := strings.TrimSpace(os.Getenv("AEGISEDGE_COORDINATION_ENABLED")); coordStr != "" {
		if strings.ToLower(coordStr) == "false" || coordStr == "0" {
			coordinationEnabled = false
		}
	}

	heartbeatInterval := DefaultHeartbeatInterval
	if hbStr := strings.TrimSpace(os.Getenv("AEGISEDGE_HEARTBEAT_INTERVAL")); hbStr != "" {
		if parsed, err := time.ParseDuration(hbStr); err == nil && parsed > 0 {
			heartbeatInterval = parsed
		}
	}

	initialReconnectInterval := DefaultInitialReconnectInterval
	if recStr := strings.TrimSpace(os.Getenv("AEGISEDGE_RECONNECT_INTERVAL")); recStr != "" {
		if parsed, err := time.ParseDuration(recStr); err == nil && parsed > 0 {
			initialReconnectInterval = parsed
		}
	}

	maxReconnectInterval := DefaultMaxReconnectInterval
	if maxRecStr := strings.TrimSpace(os.Getenv("AEGISEDGE_MAX_RECONNECT_INTERVAL")); maxRecStr != "" {
		if parsed, err := time.ParseDuration(maxRecStr); err == nil && parsed > 0 {
			maxReconnectInterval = parsed
		}
	}

	return Config{
		NodeID:                   nodeID,
		DatabasePath:             dbPath,
		CollectionInterval:       interval,
		ControlPlaneURL:          controlPlaneURL,
		SyncInterval:             syncInterval,
		SyncEnabled:              syncEnabled,
		RunOnce:                  false,
		NATSEnabled:              natsEnabled,
		NATSURL:                  natsURL,
		NATSPublishTimeout:       natsPublishTimeout,
		ModelStoreDir:            modelStoreDir,
		MetricsEnabled:           metricsEnabled,
		MetricsAddr:              metricsAddr,
		ShutdownTimeout:          shutdownTimeout,
		CoordinationEnabled:      coordinationEnabled,
		HeartbeatInterval:        heartbeatInterval,
		InitialReconnectInterval: initialReconnectInterval,
		MaxReconnectInterval:     maxReconnectInterval,
	}
}
