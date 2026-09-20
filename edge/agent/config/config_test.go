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
	os.Unsetenv("AEGISEDGE_CONTROL_PLANE_URL")
	os.Unsetenv("AEGISEDGE_SYNC_INTERVAL")
	os.Unsetenv("AEGISEDGE_SYNC_ENABLED")

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
	if cfg.ControlPlaneURL != DefaultControlPlaneURL {
		t.Errorf("expected default ControlPlaneURL %s, got %s", DefaultControlPlaneURL, cfg.ControlPlaneURL)
	}
	if cfg.SyncInterval != DefaultSyncInterval {
		t.Errorf("expected default SyncInterval %v, got %v", DefaultSyncInterval, cfg.SyncInterval)
	}
	if !cfg.SyncEnabled {
		t.Errorf("expected default SyncEnabled to be true")
	}
}

func TestLoadFromEnv_Overrides(t *testing.T) {
	os.Setenv("AEGISEDGE_NODE_ID", "custom-edge-99")
	os.Setenv("AEGISEDGE_DATABASE_PATH", "/tmp/custom.db")
	os.Setenv("AEGISEDGE_COLLECTION_INTERVAL", "250ms")
	os.Setenv("AEGISEDGE_CONTROL_PLANE_URL", "http://cp.internal:9000")
	os.Setenv("AEGISEDGE_SYNC_INTERVAL", "1s")
	os.Setenv("AEGISEDGE_SYNC_ENABLED", "false")
	defer func() {
		os.Unsetenv("AEGISEDGE_NODE_ID")
		os.Unsetenv("AEGISEDGE_DATABASE_PATH")
		os.Unsetenv("AEGISEDGE_COLLECTION_INTERVAL")
		os.Unsetenv("AEGISEDGE_CONTROL_PLANE_URL")
		os.Unsetenv("AEGISEDGE_SYNC_INTERVAL")
		os.Unsetenv("AEGISEDGE_SYNC_ENABLED")
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
	if cfg.ControlPlaneURL != "http://cp.internal:9000" {
		t.Errorf("expected custom ControlPlaneURL, got %s", cfg.ControlPlaneURL)
	}
	if cfg.SyncInterval != 1*time.Second {
		t.Errorf("expected custom SyncInterval 1s, got %v", cfg.SyncInterval)
	}
	if cfg.SyncEnabled {
		t.Errorf("expected SyncEnabled to be false")
	}
}
