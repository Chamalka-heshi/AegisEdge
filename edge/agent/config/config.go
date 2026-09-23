package config

import (
	"os"
	"strings"
	"time"
)

const (
	DefaultNodeID             = "edge-node-01"
	DefaultDatabasePath       = "data/aegisedge-edge.db"
	DefaultCollectionInterval = 5 * time.Second
	DefaultControlPlaneURL    = "http://localhost:8080"
	DefaultSyncInterval       = 5 * time.Second
	DefaultNATSURL            = "nats://127.0.0.1:4222"
	DefaultNATSPublishTimeout = 5 * time.Second
)

// Config represents runtime configuration parameters for the edge agent.
type Config struct {
	NodeID             string
	DatabasePath       string
	CollectionInterval time.Duration
	ControlPlaneURL    string
	SyncInterval       time.Duration
	SyncEnabled        bool
	RunOnce            bool
	NATSEnabled        bool
	NATSURL            string
	NATSPublishTimeout time.Duration
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

	return Config{
		NodeID:             nodeID,
		DatabasePath:       dbPath,
		CollectionInterval: interval,
		ControlPlaneURL:    controlPlaneURL,
		SyncInterval:       syncInterval,
		SyncEnabled:        syncEnabled,
		RunOnce:            false,
		NATSEnabled:        natsEnabled,
		NATSURL:            natsURL,
		NATSPublishTimeout: natsPublishTimeout,
	}
}
