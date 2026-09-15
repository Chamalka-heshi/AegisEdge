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
)

// Config represents runtime configuration parameters for the edge agent.
type Config struct {
	NodeID             string
	DatabasePath       string
	CollectionInterval time.Duration
	RunOnce            bool
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

	return Config{
		NodeID:             nodeID,
		DatabasePath:       dbPath,
		CollectionInterval: interval,
		RunOnce:            false,
	}
}
