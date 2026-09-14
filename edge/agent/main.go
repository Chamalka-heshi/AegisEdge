package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

const (
	Version = "0.1.0-dev"
	AppName = "aegisedge-agent"
)

// AgentConfig holds foundational startup parameters for the edge agent.
type AgentConfig struct {
	NodeID          string
	ControlPlaneURL string
}

func main() {
	var (
		showVersion = flag.Bool("version", false, "Print version and exit")
		nodeID      = flag.String("node-id", "edge-default", "Unique identifier for this edge node")
		cpURL       = flag.String("control-plane-url", "http://localhost:8080", "Target control plane endpoint")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("%s version %s\n", AppName, Version)
		os.Exit(0)
	}

	cfg := AgentConfig{
		NodeID:          *nodeID,
		ControlPlaneURL: *cpURL,
	}

	// Validate node identity using shared types contract
	reg := types.NodeRegistration{
		NodeID:       cfg.NodeID,
		Hostname:     "localhost",
		OS:           "edge",
		Architecture: "amd64",
	}
	_ = reg // Foundation established; full collector and sync worker will be implemented in subsequent milestones

	fmt.Printf("[%s] Starting edge agent foundation (NodeID: %s, ControlPlane: %s)...\n", AppName, cfg.NodeID, cfg.ControlPlaneURL)
}
