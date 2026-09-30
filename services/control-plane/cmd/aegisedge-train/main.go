package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/detector"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/training"
)

const (
	CLIVersion = "0.1.0-dev"
	AppName    = "aegisedge-train"
)

func main() {
	exitCode := Run(os.Args[1:], os.Stdout, os.Stderr)
	os.Exit(exitCode)
}

// Run executes the aegisedge-train CLI command with provided arguments and I/O streams.
// Returns 0 on success, 1 on failure.
func Run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet(AppName, flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		inputPath   = fs.String("input", "", "Path to input telemetry CSV training dataset (required)")
		outputPath  = fs.String("output", "", "Path to destination JSON model artifact (required)")
		trees       = fs.Int("trees", 100, "Number of isolation trees to construct (1..1000)")
		sampleSize  = fs.Int("sample-size", 256, "Subsample size psi drawn per tree (2..10000)")
		seed        = fs.Int64("seed", 42, "Deterministic pseudo-random generator seed")
		threshold   = fs.Float64("threshold", 0.60, "Anomaly decision threshold tau in (0.0, 1.0)")
		modelID     = fs.String("model-id", "iforest-v1", "Model identifier string")
		modelVer    = fs.String("model-version", "1.0.0", "Model SemVer version string")
		datasetID   = fs.String("dataset-id", "ds-baseline", "Identifier of the source training dataset")
		createdAt   = fs.String("created-at", "2026-01-01T00:00:00Z", "Deterministic model creation RFC3339 timestamp")
		showVersion = fs.Bool("version", false, "Print tool version and exit")
	)

	fs.Usage = func() {
		fmt.Fprintf(stderr, "AegisEdge Offline Isolation Forest Training CLI (%s)\n\n", AppName)
		fmt.Fprintf(stderr, "Usage:\n  %s --input <dataset.csv> --output <model.json> [flags]\n\n", AppName)
		fmt.Fprintf(stderr, "Flags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if *showVersion {
		fmt.Fprintf(stdout, "%s version %s\n", AppName, CLIVersion)
		return 0
	}

	if strings.TrimSpace(*inputPath) == "" {
		fmt.Fprintf(stderr, "Error: --input dataset path is required\n")
		fs.Usage()
		return 1
	}

	if strings.TrimSpace(*outputPath) == "" {
		fmt.Fprintf(stderr, "Error: --output model path is required\n")
		fs.Usage()
		return 1
	}

	createdTime, err := time.Parse(time.RFC3339, strings.TrimSpace(*createdAt))
	if err != nil {
		fmt.Fprintf(stderr, "Error: invalid --created-at timestamp format (must be RFC3339): %v\n", err)
		return 1
	}

	cfg := training.TrainConfig{
		ModelID:           strings.TrimSpace(*modelID),
		ModelVersion:      strings.TrimSpace(*modelVer),
		TrainingDatasetID: strings.TrimSpace(*datasetID),
		Trees:             *trees,
		SubSampleSize:     *sampleSize,
		Seed:              *seed,
		DecisionThreshold: *threshold,
		CreatedAt:         createdTime.UTC(),
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "Error: invalid training configuration: %v\n", err)
		return 1
	}

	// 1. Load CSV dataset
	dataset, err := training.LoadCSVDatasetFile(*inputPath, cfg.TrainingDatasetID)
	if err != nil {
		fmt.Fprintf(stderr, "Error: failed to load training dataset: %v\n", err)
		return 1
	}

	// 2. Train Isolation Forest model
	manifest, err := training.Train(dataset, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "Error: model training failed: %v\n", err)
		return 1
	}

	// 3. Serialize and save model artifact (includes read-back verification)
	if err := training.SaveModelToFile(manifest, *outputPath); err != nil {
		fmt.Fprintf(stderr, "Error: failed to save model artifact: %v\n", err)
		return 1
	}

	// 4. Report training results
	fmt.Fprintf(stdout, "==================================================\n")
	fmt.Fprintf(stdout, " AegisEdge Isolation Forest Offline Training Summary\n")
	fmt.Fprintf(stdout, "==================================================\n")
	fmt.Fprintf(stdout, "  Input Dataset:       %s\n", *inputPath)
	fmt.Fprintf(stdout, "  Dataset ID:          %s\n", manifest.TrainingDatasetID)
	fmt.Fprintf(stdout, "  Sample Count:        %d\n", len(dataset.Rows))
	fmt.Fprintf(stdout, "  Feature Dimensions:  %d (%s)\n", manifest.InputDimensions, detector.FeatureSchemaV1)
	fmt.Fprintf(stdout, "  Features:            %s\n", strings.Join(manifest.SupportedMetrics, ", "))
	fmt.Fprintf(stdout, "  Tree Count:          %d\n", len(manifest.Trees))
	fmt.Fprintf(stdout, "  Subsample Size (psi): %d\n", manifest.SubSampleSize)
	fmt.Fprintf(stdout, "  Random Seed:         %d\n", cfg.Seed)
	fmt.Fprintf(stdout, "  Decision Threshold:  %.4f\n", manifest.DecisionThreshold)
	fmt.Fprintf(stdout, "  Model ID:            %s\n", manifest.ModelID)
	fmt.Fprintf(stdout, "  Model Version:       %s\n", manifest.ModelVersion)
	fmt.Fprintf(stdout, "  Model Status:        %s\n", manifest.Status)
	fmt.Fprintf(stdout, "  SHA-256 Checksum:    %s\n", manifest.ChecksumSHA256)
	fmt.Fprintf(stdout, "  Output Artifact:     %s\n", *outputPath)
	fmt.Fprintf(stdout, "--------------------------------------------------\n")
	fmt.Fprintf(stdout, "  Validation Result:   PASSED (round-trip verified)\n")
	fmt.Fprintf(stdout, "==================================================\n")

	return 0
}
