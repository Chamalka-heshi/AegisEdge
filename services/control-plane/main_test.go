package main

import (
	"testing"
)

func TestServerConfig_Defaults(t *testing.T) {
	cfg := ServerConfig{
		Port:     8080,
		LogLevel: "info",
	}

	if cfg.Port != 8080 {
		t.Errorf("expected Port 8080, got %d", cfg.Port)
	}

	if cfg.LogLevel != "info" {
		t.Errorf("expected LogLevel 'info', got %q", cfg.LogLevel)
	}
}
