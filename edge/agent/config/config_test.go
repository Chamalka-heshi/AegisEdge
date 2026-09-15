package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadFromEnv_Defaults(t *testing.T) {
	// Ensure relevant environment variables are clear
	os.Unsetenv("AEGISEDGE_NODE_ID")
	os.Unsetenv("AEGISEDGE_DATABASE_PATH")
	os.Unsetenv("AEGISEDGE_COLLECTION_INTERVAL")

	cfg := LoadFromEnv()

	if cfg.NodeID != DefaultNodeID {
		t.Errorf("expected default NodeID %s, got %s", DefaultNodeID, cfg.NodeID)
	}
	if cfg.DatabasePath != DefaultDatabasePath {
		t.Errorf("expected default DatabasePath %s, got %s", DefaultDatabasePath, cfg.DatabasePath)
	}
	if cfg.CollectionInterval != DefaultCollectionInterval {
		t.Errorf("expected default interval %v, got %v", DefaultCollectionInterval, cfg.CollectionInterval)
	}
}

func TestLoadFromEnv_Overrides(t *testing.T) {
	os.Setenv("AEGISEDGE_NODE_ID", "custom-edge-99")
	os.Setenv("AEGISEDGE_DATABASE_PATH", "/tmp/custom.db")
	os.Setenv("AEGISEDGE_COLLECTION_INTERVAL", "250ms")
	defer func() {
		os.Unsetenv("AEGISEDGE_NODE_ID")
		os.Unsetenv("AEGISEDGE_DATABASE_PATH")
		os.Unsetenv("AEGISEDGE_COLLECTION_INTERVAL")
	}()

	cfg := LoadFromEnv()

	if cfg.NodeID != "custom-edge-99" {
		t.Errorf("expected custom NodeID, got %s", cfg.NodeID)
	}
	if cfg.DatabasePath != "/tmp/custom.db" {
		t.Errorf("expected custom DatabasePath, got %s", cfg.DatabasePath)
	}
	if cfg.CollectionInterval != 250*time.Millisecond {
		t.Errorf("expected 250ms, got %v", cfg.CollectionInterval)
	}
}
