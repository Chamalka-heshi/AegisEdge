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
	if cfg.ModelStoreDir != DefaultModelStoreDir {
		t.Errorf("expected default ModelStoreDir %s, got %s", DefaultModelStoreDir, cfg.ModelStoreDir)
	}
	if cfg.MetricsEnabled != DefaultMetricsEnabled {
		t.Errorf("expected default MetricsEnabled to be false")
	}
	if cfg.MetricsAddr != DefaultMetricsAddr {
		t.Errorf("expected default MetricsAddr %s, got %s", DefaultMetricsAddr, cfg.MetricsAddr)
	}
	if !cfg.CoordinationEnabled {
		t.Errorf("expected default CoordinationEnabled to be true")
	}
	if cfg.HeartbeatInterval != DefaultHeartbeatInterval {
		t.Errorf("expected default HeartbeatInterval %v, got %v", DefaultHeartbeatInterval, cfg.HeartbeatInterval)
	}
	if cfg.InitialReconnectInterval != DefaultInitialReconnectInterval {
		t.Errorf("expected default InitialReconnectInterval %v, got %v", DefaultInitialReconnectInterval, cfg.InitialReconnectInterval)
	}
	if cfg.MaxReconnectInterval != DefaultMaxReconnectInterval {
		t.Errorf("expected default MaxReconnectInterval %v, got %v", DefaultMaxReconnectInterval, cfg.MaxReconnectInterval)
	}
	if cfg.RetentionEnabled != DefaultRetentionEnabled {
		t.Errorf("expected default RetentionEnabled %v, got %v", DefaultRetentionEnabled, cfg.RetentionEnabled)
	}
	if cfg.RetentionPeriod != DefaultRetentionPeriod {
		t.Errorf("expected default RetentionPeriod %v, got %v", DefaultRetentionPeriod, cfg.RetentionPeriod)
	}
	if cfg.PruneInterval != DefaultPruneInterval {
		t.Errorf("expected default PruneInterval %v, got %v", DefaultPruneInterval, cfg.PruneInterval)
	}
	if cfg.PruneLimit != DefaultPruneLimit {
		t.Errorf("expected default PruneLimit %v, got %v", DefaultPruneLimit, cfg.PruneLimit)
	}
}

func TestLoadFromEnv_Overrides(t *testing.T) {
	os.Setenv("AEGISEDGE_NODE_ID", "custom-edge-99")
	os.Setenv("AEGISEDGE_DATABASE_PATH", "/tmp/custom.db")
	os.Setenv("AEGISEDGE_COLLECTION_INTERVAL", "250ms")
	os.Setenv("AEGISEDGE_CONTROL_PLANE_URL", "http://cp.internal:9000")
	os.Setenv("AEGISEDGE_SYNC_INTERVAL", "1s")
	os.Setenv("AEGISEDGE_SYNC_ENABLED", "false")
	os.Setenv("AEGISEDGE_MODEL_STORE_DIR", "/var/lib/aegisedge/models")
	os.Setenv("METRICS_ENABLED", "true")
	os.Setenv("METRICS_ADDR", "127.0.0.1:9999")
	os.Setenv("AEGISEDGE_COORDINATION_ENABLED", "false")
	os.Setenv("AEGISEDGE_HEARTBEAT_INTERVAL", "15s")
	os.Setenv("AEGISEDGE_RECONNECT_INTERVAL", "2s")
	os.Setenv("AEGISEDGE_MAX_RECONNECT_INTERVAL", "45s")
	os.Setenv("AEGISEDGE_RETENTION_ENABLED", "true")
	os.Setenv("AEGISEDGE_RETENTION_PERIOD", "48h")
	os.Setenv("AEGISEDGE_PRUNE_INTERVAL", "30m")
	os.Setenv("AEGISEDGE_PRUNE_LIMIT", "250")
	defer func() {
		os.Unsetenv("AEGISEDGE_NODE_ID")
		os.Unsetenv("AEGISEDGE_DATABASE_PATH")
		os.Unsetenv("AEGISEDGE_COLLECTION_INTERVAL")
		os.Unsetenv("AEGISEDGE_CONTROL_PLANE_URL")
		os.Unsetenv("AEGISEDGE_SYNC_INTERVAL")
		os.Unsetenv("AEGISEDGE_SYNC_ENABLED")
		os.Unsetenv("AEGISEDGE_MODEL_STORE_DIR")
		os.Unsetenv("METRICS_ENABLED")
		os.Unsetenv("METRICS_ADDR")
		os.Unsetenv("AEGISEDGE_COORDINATION_ENABLED")
		os.Unsetenv("AEGISEDGE_HEARTBEAT_INTERVAL")
		os.Unsetenv("AEGISEDGE_RECONNECT_INTERVAL")
		os.Unsetenv("AEGISEDGE_MAX_RECONNECT_INTERVAL")
		os.Unsetenv("AEGISEDGE_RETENTION_ENABLED")
		os.Unsetenv("AEGISEDGE_RETENTION_PERIOD")
		os.Unsetenv("AEGISEDGE_PRUNE_INTERVAL")
		os.Unsetenv("AEGISEDGE_PRUNE_LIMIT")
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
	if cfg.ModelStoreDir != "/var/lib/aegisedge/models" {
		t.Errorf("expected custom ModelStoreDir, got %s", cfg.ModelStoreDir)
	}
	if !cfg.MetricsEnabled {
		t.Errorf("expected MetricsEnabled to be true")
	}
	if cfg.MetricsAddr != "127.0.0.1:9999" {
		t.Errorf("expected custom MetricsAddr 127.0.0.1:9999, got %s", cfg.MetricsAddr)
	}
	if cfg.CoordinationEnabled {
		t.Errorf("expected CoordinationEnabled to be false")
	}
	if cfg.HeartbeatInterval != 15*time.Second {
		t.Errorf("expected custom HeartbeatInterval 15s, got %v", cfg.HeartbeatInterval)
	}
	if cfg.InitialReconnectInterval != 2*time.Second {
		t.Errorf("expected custom InitialReconnectInterval 2s, got %v", cfg.InitialReconnectInterval)
	}
	if cfg.MaxReconnectInterval != 45*time.Second {
		t.Errorf("expected custom MaxReconnectInterval 45s, got %v", cfg.MaxReconnectInterval)
	}
	if !cfg.RetentionEnabled {
		t.Errorf("expected RetentionEnabled to be true")
	}
	if cfg.RetentionPeriod != 48*time.Hour {
		t.Errorf("expected RetentionPeriod 48h, got %v", cfg.RetentionPeriod)
	}
	if cfg.PruneInterval != 30*time.Minute {
		t.Errorf("expected PruneInterval 30m, got %v", cfg.PruneInterval)
	}
	if cfg.PruneLimit != 250 {
		t.Errorf("expected PruneLimit 250, got %d", cfg.PruneLimit)
	}
}
