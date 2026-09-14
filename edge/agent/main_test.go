package main

import (
	"testing"
)

func TestAgentConfig_Defaults(t *testing.T) {
	cfg := AgentConfig{
		NodeID:          "test-node",
		ControlPlaneURL: "http://localhost:8080",
	}

	if cfg.NodeID != "test-node" {
		t.Errorf("expected NodeID 'test-node', got %q", cfg.NodeID)
	}

	if cfg.ControlPlaneURL != "http://localhost:8080" {
		t.Errorf("expected ControlPlaneURL 'http://localhost:8080', got %q", cfg.ControlPlaneURL)
	}
}
