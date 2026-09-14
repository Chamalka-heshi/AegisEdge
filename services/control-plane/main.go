package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

const (
	Version = "0.1.0-dev"
	AppName = "aegisedge-control-plane"
)

// ServerConfig holds foundational configuration for the control plane.
type ServerConfig struct {
	Port     int
	LogLevel string
}

func main() {
	var (
		showVersion = flag.Bool("version", false, "Print version and exit")
		port        = flag.Int("port", 8080, "HTTP server listening port")
		logLevel    = flag.String("log-level", "info", "Logging level (debug, info, warn, error)")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("%s version %s\n", AppName, Version)
		os.Exit(0)
	}

	cfg := ServerConfig{
		Port:     *port,
		LogLevel: *logLevel,
	}

	// Verify shared types reference
	sample := types.Heartbeat{
		NodeID:         "system",
		SequenceNumber: 0,
		Status:         types.NodeStatusHealthy,
		Timestamp:      time.Now().UTC(),
	}
	_ = sample

	fmt.Printf("[%s] Starting control plane foundation on port %d (LogLevel: %s)...\n", AppName, cfg.Port, cfg.LogLevel)
}
