package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/detector"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/training"
)

func TestCLI_HelpAndVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer

	// Help flag
	code := Run([]string{"--help"}, &stdout, &stderr)
	// flag.ContinueOnError returns 1 or error output on help
	if !strings.Contains(stderr.String(), "Usage:") && !strings.Contains(stdout.String(), "Usage:") {
		t.Fatalf("expected usage output, got: stdout=%q, stderr=%q", stdout.String(), stderr.String())
	}

	// Version flag
	stdout.Reset()
	stderr.Reset()
	code = Run([]string{"--version"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected code 0 for --version, got %d", code)
	}
	if !strings.Contains(stdout.String(), CLIVersion) {
		t.Fatalf("expected version %s, got: %s", CLIVersion, stdout.String())
	}
}

func TestCLI_MissingFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer

	// Missing both
	code := Run([]string{}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("expected code 1 on missing flags, got %d", code)
	}
	if !strings.Contains(stderr.String(), "--input dataset path is required") {
		t.Fatalf("expected missing input error, got: %s", stderr.String())
	}

	// Missing output
	stdout.Reset()
	stderr.Reset()
	code = Run([]string{"--input", "some.csv"}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("expected code 1 on missing output, got %d", code)
	}
	if !strings.Contains(stderr.String(), "--output model path is required") {
		t.Fatalf("expected missing output error, got: %s", stderr.String())
	}
}

func TestCLI_EndToEndExecution(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "telemetry_train.csv")
	modelPath := filepath.Join(tmpDir, "model_output.json")

	csvContent := `cpu_usage_percent,memory_usage_percent,disk_usage_percent,temperature_celsius
42.0,55.0,32.0,46.0
45.0,58.0,34.0,48.0
48.0,60.0,36.0,50.0
50.0,62.0,38.0,52.0
52.0,64.0,40.0,54.0`

	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to write test csv: %v", err)
	}

	var stdout, stderr bytes.Buffer
	args := []string{
		"--input", csvPath,
		"--output", modelPath,
		"--trees", "10",
		"--sample-size", "4",
		"--seed", "123",
		"--threshold", "0.65",
		"--model-id", "cli-test-model",
		"--model-version", "1.0.0",
		"--dataset-id", "ds-cli-unit-test",
		"--created-at", "2026-09-30T00:00:00Z",
	}

	code := Run(args, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("CLI execution failed (code %d): stderr=%s", code, stderr.String())
	}

	outStr := stdout.String()
	if !strings.Contains(outStr, "AegisEdge Isolation Forest Offline Training Summary") {
		t.Fatalf("expected summary banner in output, got: %s", outStr)
	}
	if !strings.Contains(outStr, "Validation Result:   PASSED") {
		t.Fatalf("expected validation passed in output, got: %s", outStr)
	}
	if !strings.Contains(outStr, "Tree Count:          10") {
		t.Fatalf("expected tree count 10 in output, got: %s", outStr)
	}

	// Verify the model file on disk is valid
	manifest, err := training.LoadModelFromFile(modelPath)
	if err != nil {
		t.Fatalf("failed to load generated model file: %v", err)
	}

	if manifest.ModelID != "cli-test-model" {
		t.Fatalf("expected model ID 'cli-test-model', got: %s", manifest.ModelID)
	}
	if len(manifest.Trees) != 10 {
		t.Fatalf("expected 10 trees, got: %d", len(manifest.Trees))
	}
	if manifest.DecisionThreshold != 0.65 {
		t.Fatalf("expected threshold 0.65, got: %f", manifest.DecisionThreshold)
	}

	// Verify detector can consume it directly
	det, err := detector.NewMLDetector(detector.MLDetectorConfig{Manifest: manifest})
	if err != nil {
		t.Fatalf("MLDetector failed to load CLI-generated model: %v", err)
	}
	if det.Name() != "ml_isolation_forest" {
		t.Fatalf("detector name mismatch: %s", det.Name())
	}
}
