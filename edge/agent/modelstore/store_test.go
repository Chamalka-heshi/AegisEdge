package modelstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/detector"
	"github.com/Chamalka-heshi/AegisEdge/shared/ml"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// helperBuildValidManifest creates a fully valid, structurally sound ML model manifest.
func helperBuildValidManifest(id, version string, threshold float64) *ml.ModelManifest {
	manifest := &ml.ModelManifest{
		ModelID:               id,
		ModelVersion:          version,
		ArtifactFormatVersion: ml.ArtifactFormatV1,
		Algorithm:             ml.AlgorithmIsolationForest,
		FeatureSchemaVersion:  ml.FeatureSchemaV1,
		TrainingDatasetID:     "ds-test-dataset",
		CreatedAt:             time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		InputDimensions:       4,
		SupportedMetrics: []string{
			"cpu_usage_percent",
			"memory_usage_percent",
			"disk_usage_percent",
			"temperature_celsius",
		},
		NormalizationParams: []ml.FeatureNormalizationParams{
			{MetricName: "cpu_usage_percent", Mean: 50.0, StdDev: 10.0, Min: 0.0, Max: 100.0},
			{MetricName: "memory_usage_percent", Mean: 60.0, StdDev: 15.0, Min: 0.0, Max: 100.0},
			{MetricName: "disk_usage_percent", Mean: 40.0, StdDev: 5.0, Min: 0.0, Max: 100.0},
			{MetricName: "temperature_celsius", Mean: 50.0, StdDev: 10.0, Min: -40.0, Max: 125.0},
		},
		Trees: []ml.IsolationTree{
			{
				RootIndex: 0,
				Nodes: []ml.IsolationTreeNode{
					{FeatureIndex: 0, SplitValue: 0.0, LeftChild: 1, RightChild: 2, Size: 0},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1},
				},
			},
			{
				RootIndex: 0,
				Nodes: []ml.IsolationTreeNode{
					{FeatureIndex: 1, SplitValue: 0.0, LeftChild: 1, RightChild: 2, Size: 0},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1},
				},
			},
		},
		SubSampleSize:     256,
		DecisionThreshold: threshold,
		Status:            ml.ModelStatusActive,
	}

	checksum, err := manifest.ComputeChecksum()
	if err != nil {
		panic(fmt.Sprintf("helperBuildValidManifest checksum failure: %v", err))
	}
	manifest.ChecksumSHA256 = checksum

	return manifest
}

// helperSerializeManifest helper returns formatted JSON bytes for a manifest.
func helperSerializeManifest(t *testing.T, m *ml.ModelManifest) []byte {
	t.Helper()
	data, err := ml.Serialize(m)
	if err != nil {
		t.Fatalf("helperSerializeManifest failed: %v", err)
	}
	return data
}

// -----------------------------------------------------------------------------
// 1. Successful Staging
// -----------------------------------------------------------------------------

func TestModelStore_SuccessfulStaging(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})
	if err != nil {
		t.Fatalf("failed to create model store: %v", err)
	}

	manifest := helperBuildValidManifest("iforest-edge-v1", "1.0.0", 0.60)
	rawBytes := helperSerializeManifest(t, manifest)

	ctx := context.Background()
	staged, err := store.StageModel(ctx, StageRequest{
		Artifact: rawBytes,
	})
	if err != nil {
		t.Fatalf("expected successful staging, got: %v", err)
	}

	if staged == nil {
		t.Fatal("expected non-nil staged candidate")
	}
	if staged.Manifest.ModelID != "iforest-edge-v1" {
		t.Fatalf("expected model ID 'iforest-edge-v1', got %s", staged.Manifest.ModelID)
	}
	if staged.Manifest.ModelVersion != "1.0.0" {
		t.Fatalf("expected model version '1.0.0', got %s", staged.Manifest.ModelVersion)
	}
	if staged.ReusedExisting {
		t.Fatalf("expected newly staged model, got ReusedExisting=true")
	}
	if _, err := os.Stat(staged.ArtifactPath); os.IsNotExist(err) {
		t.Fatalf("candidate artifact file does not exist at %s", staged.ArtifactPath)
	}
	if _, err := os.Stat(staged.MetadataPath); os.IsNotExist(err) {
		t.Fatalf("candidate metadata file does not exist at %s", staged.MetadataPath)
	}
}

// -----------------------------------------------------------------------------
// 2. Malformed Artifact Rejection
// -----------------------------------------------------------------------------

func TestModelStore_MalformedArtifactRejection(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	malformedJSON := []byte(`{"model_id": "broken", "trees": [ invalid json }`)
	ctx := context.Background()

	_, err := store.StageModel(ctx, StageRequest{Artifact: malformedJSON})
	if err == nil {
		t.Fatal("expected malformed artifact rejection, got nil error")
	}
	if !errors.Is(err, ErrDeserializeFailure) {
		t.Fatalf("expected ErrDeserializeFailure, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 3. Empty Artifact Rejection
// -----------------------------------------------------------------------------

func TestModelStore_EmptyArtifactRejection(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	ctx := context.Background()
	_, err := store.StageModel(ctx, StageRequest{Artifact: []byte{}})
	if err == nil {
		t.Fatal("expected empty artifact rejection, got nil error")
	}
	if !errors.Is(err, ErrEmptyArtifact) {
		t.Fatalf("expected ErrEmptyArtifact, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 4. Checksum Mismatch Rejection
// -----------------------------------------------------------------------------

func TestModelStore_ChecksumMismatchRejection(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	manifest := helperBuildValidManifest("iforest-tamper", "1.0.0", 0.60)
	rawBytes := helperSerializeManifest(t, manifest)

	// Tamper with the checksum field
	tampered := strings.Replace(string(rawBytes), manifest.ChecksumSHA256, "0000000000000000000000000000000000000000000000000000000000000000", 1)

	ctx := context.Background()
	_, err := store.StageModel(ctx, StageRequest{Artifact: []byte(tampered)})
	if err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
	if !errors.Is(err, ErrChecksumFailure) {
		t.Fatalf("expected ErrChecksumFailure, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 5. Structural Validation Failure
// -----------------------------------------------------------------------------

func TestModelStore_StructuralValidationFailure(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	// Manifest with cycle in tree: left child points to root
	manifest := helperBuildValidManifest("iforest-cycle", "1.0.0", 0.60)
	manifest.Trees[0].Nodes[0].LeftChild = 0 // Cycle!

	// Recompute checksum so checksum passes, isolating the structural validation check
	sum, _ := manifest.ComputeChecksum()
	manifest.ChecksumSHA256 = sum

	rawBytes, _ := json.Marshal(manifest)

	ctx := context.Background()
	_, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes})
	if err == nil {
		t.Fatal("expected structural validation failure, got nil")
	}
	if !errors.Is(err, ErrValidationFailure) && !errors.Is(err, ErrDeserializeFailure) {
		t.Fatalf("expected validation/deserialize failure, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 6. Compatibility Failure
// -----------------------------------------------------------------------------

func TestModelStore_CompatibilityFailure(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	// Manifest with unsupported algorithm
	manifest := helperBuildValidManifest("iforest-algo", "1.0.0", 0.60)
	manifest.Algorithm = "random_forest" // incompatible
	sum, _ := manifest.ComputeChecksum()
	manifest.ChecksumSHA256 = sum
	rawBytes, _ := json.Marshal(manifest)

	ctx := context.Background()
	_, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes})
	if err == nil {
		t.Fatal("expected compatibility failure for algorithm, got nil")
	}
	if !errors.Is(err, ErrCompatibilityFailure) && !errors.Is(err, ErrValidationFailure) && !errors.Is(err, ErrDeserializeFailure) {
		t.Fatalf("expected compatibility/validation error, got: %v", err)
	}

	// Manifest with dimension requirement mismatch
	manifest2 := helperBuildValidManifest("iforest-dim", "1.0.0", 0.60)
	strictReq := ml.DefaultCompatibilityRequirements()
	strictReq.ExpectedDimensions = 8 // expect 8, but manifest has 4

	rawBytes2 := helperSerializeManifest(t, manifest2)
	_, err2 := store.StageModel(ctx, StageRequest{
		Artifact:         rawBytes2,
		CompatibilityReq: &strictReq,
	})
	if err2 == nil {
		t.Fatal("expected compatibility failure for dimension mismatch, got nil")
	}
	if !errors.Is(err2, ErrCompatibilityFailure) {
		t.Fatalf("expected ErrCompatibilityFailure, got: %v", err2)
	}
}

// -----------------------------------------------------------------------------
// 7. Oversized Artifact Rejection
// -----------------------------------------------------------------------------

func TestModelStore_OversizedArtifactRejection(t *testing.T) {
	tmpDir := t.TempDir()
	// Set very small limit: 100 bytes
	store, _ := NewFileSystemModelStore(StoreConfig{
		BaseDir:              tmpDir,
		MaxArtifactSizeBytes: 100,
	})

	manifest := helperBuildValidManifest("iforest-big", "1.0.0", 0.60)
	rawBytes := helperSerializeManifest(t, manifest) // ~1.5 KB

	ctx := context.Background()
	_, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes})
	if err == nil {
		t.Fatal("expected oversized artifact rejection, got nil")
	}
	if !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatalf("expected ErrArtifactTooLarge, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 8. Invalid Model Version
// -----------------------------------------------------------------------------

func TestModelStore_InvalidModelVersion(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	manifest := helperBuildValidManifest("iforest-v", "", 0.60) // empty version
	sum, _ := manifest.ComputeChecksum()
	manifest.ChecksumSHA256 = sum
	rawBytes, _ := json.Marshal(manifest)

	ctx := context.Background()
	_, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes})
	if err == nil {
		t.Fatal("expected empty version rejection, got nil")
	}
	if !errors.Is(err, ErrValidationFailure) && !errors.Is(err, ErrDeserializeFailure) {
		t.Fatalf("expected validation error, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 9. Duplicate Identical Staging (Idempotency)
// -----------------------------------------------------------------------------

func TestModelStore_DuplicateIdenticalStaging_Idempotent(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	manifest := helperBuildValidManifest("iforest-idemp", "1.0.0", 0.60)
	rawBytes := helperSerializeManifest(t, manifest)

	ctx := context.Background()
	first, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes})
	if err != nil {
		t.Fatalf("first staging failed: %v", err)
	}
	if first.ReusedExisting {
		t.Fatal("first staging should not have ReusedExisting=true")
	}

	// Stage exact same artifact a second time
	second, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes})
	if err != nil {
		t.Fatalf("second staging should succeed idempotently, got error: %v", err)
	}
	if !second.ReusedExisting {
		t.Fatal("second staging must report ReusedExisting=true")
	}
	if second.Manifest.ChecksumSHA256 != first.Manifest.ChecksumSHA256 {
		t.Fatalf("checksum mismatch on reused candidate: %s != %s",
			second.Manifest.ChecksumSHA256, first.Manifest.ChecksumSHA256)
	}
}

// -----------------------------------------------------------------------------
// 10. Conflicting Same-Identity Artifact
// -----------------------------------------------------------------------------

func TestModelStore_ConflictingSameIdentityArtifact_Rejected(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	// Model 1: id="iforest-conflict", ver="1.0.0", threshold=0.60
	m1 := helperBuildValidManifest("iforest-conflict", "1.0.0", 0.60)
	rawBytes1 := helperSerializeManifest(t, m1)

	ctx := context.Background()
	_, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes1})
	if err != nil {
		t.Fatalf("first staging failed: %v", err)
	}

	// Model 2: same id="iforest-conflict", same ver="1.0.0", but threshold=0.75 (different checksum)
	m2 := helperBuildValidManifest("iforest-conflict", "1.0.0", 0.75)
	rawBytes2 := helperSerializeManifest(t, m2)

	if m1.ChecksumSHA256 == m2.ChecksumSHA256 {
		t.Fatal("test setup error: m1 and m2 must have different checksums")
	}

	// Second staging with different checksum MUST be rejected
	_, err = store.StageModel(ctx, StageRequest{Artifact: rawBytes2})
	if err == nil {
		t.Fatal("expected conflict rejection for different checksum, got nil")
	}
	if !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("expected ErrCandidateConflict, got: %v", err)
	}

	// Verify original candidate was NOT overwritten
	current, err := store.GetCandidate(ctx)
	if err != nil {
		t.Fatalf("failed to retrieve current candidate: %v", err)
	}
	if current.Manifest.ChecksumSHA256 != m1.ChecksumSHA256 {
		t.Fatalf("candidate was modified! got %s, want %s", current.Manifest.ChecksumSHA256, m1.ChecksumSHA256)
	}
}

// -----------------------------------------------------------------------------
// 11. Temporary File Cleanup After Failure
// -----------------------------------------------------------------------------

func TestModelStore_TemporaryFileCleanupAfterFailure(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	stagingDir := filepath.Join(tmpDir, DirStaging)

	// Attempt to stage invalid JSON
	ctx := context.Background()
	_, _ = store.StageModel(ctx, StageRequest{Artifact: []byte("{not json")})

	// Verify staging directory is clean
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		t.Fatalf("failed to read staging dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 files in staging directory after failure, found %d", len(entries))
	}
}

// -----------------------------------------------------------------------------
// 12. Staged Artifact Can Be Read Back
// -----------------------------------------------------------------------------

func TestModelStore_StagedArtifactCanBeReadBack(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	manifest := helperBuildValidManifest("iforest-readback", "2.1.0", 0.55)
	rawBytes := helperSerializeManifest(t, manifest)

	ctx := context.Background()
	staged, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes})
	if err != nil {
		t.Fatalf("staging failed: %v", err)
	}

	// Read candidate via GetCandidate
	retrieved, err := store.GetCandidate(ctx)
	if err != nil {
		t.Fatalf("GetCandidate failed: %v", err)
	}

	if retrieved.Manifest.ModelID != staged.Manifest.ModelID {
		t.Fatalf("model ID mismatch: %s != %s", retrieved.Manifest.ModelID, staged.Manifest.ModelID)
	}
	if retrieved.Manifest.ModelVersion != staged.Manifest.ModelVersion {
		t.Fatalf("model version mismatch: %s != %s", retrieved.Manifest.ModelVersion, staged.Manifest.ModelVersion)
	}
	if retrieved.Manifest.ChecksumSHA256 != staged.Manifest.ChecksumSHA256 {
		t.Fatalf("checksum mismatch: %s != %s", retrieved.Manifest.ChecksumSHA256, staged.Manifest.ChecksumSHA256)
	}
	if retrieved.Metadata.ArtifactSizeBytes != int64(len(rawBytes)) {
		t.Fatalf("metadata size mismatch: %d != %d", retrieved.Metadata.ArtifactSizeBytes, len(rawBytes))
	}
}

// -----------------------------------------------------------------------------
// 13. Staged Artifact Checksum Remains Valid
// -----------------------------------------------------------------------------

func TestModelStore_StagedArtifactChecksumRemainsValid(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	manifest := helperBuildValidManifest("iforest-check", "1.0.0", 0.60)
	rawBytes := helperSerializeManifest(t, manifest)

	ctx := context.Background()
	staged, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes})
	if err != nil {
		t.Fatalf("staging failed: %v", err)
	}

	// Read file directly from disk and recompute checksum
	diskBytes, err := os.ReadFile(staged.ArtifactPath)
	if err != nil {
		t.Fatalf("failed to read disk file %s: %v", staged.ArtifactPath, err)
	}

	loadedManifest, err := ml.Deserialize(diskBytes)
	if err != nil {
		t.Fatalf("failed to deserialize disk artifact: %v", err)
	}

	recomputed, err := loadedManifest.ComputeChecksum()
	if err != nil {
		t.Fatalf("failed to recompute checksum: %v", err)
	}

	if recomputed != manifest.ChecksumSHA256 {
		t.Fatalf("recomputed checksum mismatch: %s != %s", recomputed, manifest.ChecksumSHA256)
	}
}

// -----------------------------------------------------------------------------
// 14. Active Model Remains Unchanged Before and After Staging
// -----------------------------------------------------------------------------

func TestModelStore_ActiveModelRemainsUnchanged(t *testing.T) {
	// Initialize an active MLDetector with baseline model
	activeManifest := helperBuildValidManifest("active-model-v1", "1.0.0", 0.50)
	activeDetector, err := detector.NewMLDetector(detector.MLDetectorConfig{
		Manifest: activeManifest,
	})
	if err != nil {
		t.Fatalf("failed to initialize active detector: %v", err)
	}

	// Sample test vectors
	testVectors := [][]float64{
		{50.0, 60.0, 40.0, 50.0}, // nominal baseline vector
		{90.0, 60.0, 40.0, 50.0}, // high CPU
		{95.0, 95.0, 95.0, 95.0}, // multivariate anomaly
	}

	ctx := context.Background()
	evalTime := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	// Record pre-staging baseline signals
	preScores := make([]float64, len(testVectors))
	preSignals := make([]*types.AnomalySignal, len(testVectors))
	for i, vec := range testVectors {
		sig, err := activeDetector.DetectVector(ctx, "edge-node-01", evalTime, fmt.Sprintf("sample-%d", i), vec)
		if err != nil {
			t.Fatalf("pre-staging inference error: %v", err)
		}
		if sig != nil {
			preScores[i] = sig.AnomalyScore
			preSignals[i] = sig
		}
	}

	// Initialize ModelStore and stage a COMPLETELY DIFFERENT candidate model
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	candidateManifest := helperBuildValidManifest("candidate-v2", "2.0.0", 0.80)
	candidateBytes := helperSerializeManifest(t, candidateManifest)

	staged, err := store.StageModel(ctx, StageRequest{Artifact: candidateBytes})
	if err != nil {
		t.Fatalf("candidate staging failed: %v", err)
	}
	if staged.Manifest.ModelID != "candidate-v2" {
		t.Fatalf("unexpected candidate ID: %s", staged.Manifest.ModelID)
	}

	// Verify active detector metadata is UNCHANGED
	if activeDetector.Name() != "ml_isolation_forest" {
		t.Fatalf("active detector name changed: %s", activeDetector.Name())
	}
	if activeDetector.Version() != "1.0.0" {
		t.Fatalf("active detector version changed: %s", activeDetector.Version())
	}
	if activeDetector.Model().ModelID != "active-model-v1" {
		t.Fatalf("active detector model ID changed: %s", activeDetector.Model().ModelID)
	}

	// Evaluate post-staging inference on the SAME active detector
	for i, vec := range testVectors {
		sig, err := activeDetector.DetectVector(ctx, "edge-node-01", evalTime, fmt.Sprintf("sample-%d", i), vec)
		if err != nil {
			t.Fatalf("post-staging inference error: %v", err)
		}

		if (preSignals[i] == nil && sig != nil) || (preSignals[i] != nil && sig == nil) {
			t.Fatalf("vector %d anomaly decision mismatch after staging: pre=%v, post=%v", i, preSignals[i], sig)
		}

		if sig != nil && preSignals[i] != nil {
			if sig.AnomalyScore != preScores[i] {
				t.Fatalf("vector %d anomaly score mismatch: pre=%f, post=%f", i, preScores[i], sig.AnomalyScore)
			}
			if sig.AnomalyID != preSignals[i].AnomalyID {
				t.Fatalf("vector %d anomaly ID mismatch: pre=%s, post=%s", i, preSignals[i].AnomalyID, sig.AnomalyID)
			}
		}
	}
}

// -----------------------------------------------------------------------------
// 15. Concurrent Staging Behavior
// -----------------------------------------------------------------------------

func TestModelStore_ConcurrentStaging(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	manifest := helperBuildValidManifest("iforest-concurrent", "1.0.0", 0.60)
	rawBytes := helperSerializeManifest(t, manifest)

	const numGoroutines = 10
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)
	stagedCh := make(chan *StagedCandidate, numGoroutines)

	ctx := context.Background()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			candidate, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes})
			if err != nil {
				errCh <- err
				return
			}
			stagedCh <- candidate
		}()
	}

	wg.Wait()
	close(errCh)
	close(stagedCh)

	// All concurrent staging attempts of the exact same artifact must succeed (either initial or idempotent)
	for err := range errCh {
		t.Fatalf("concurrent staging error: %v", err)
	}

	candidates := make([]*StagedCandidate, 0, numGoroutines)
	for c := range stagedCh {
		candidates = append(candidates, c)
	}

	if len(candidates) != numGoroutines {
		t.Fatalf("expected %d candidates, got %d", numGoroutines, len(candidates))
	}

	// Verify all returned candidate checksums are identical
	for _, c := range candidates {
		if c.Manifest.ChecksumSHA256 != manifest.ChecksumSHA256 {
			t.Fatalf("checksum mismatch in concurrent candidate: %s != %s",
				c.Manifest.ChecksumSHA256, manifest.ChecksumSHA256)
		}
	}
}

// -----------------------------------------------------------------------------
// 16. Windows-Compatible Filesystem Behavior
// -----------------------------------------------------------------------------

func TestModelStore_WindowsCompatibleFilesystemBehavior(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileSystemModelStore(StoreConfig{BaseDir: tmpDir})

	// Test 16A: Staging with Windows path separators
	manifest1 := helperBuildValidManifest("iforest-win-1", "1.0.0", 0.60)
	rawBytes1 := helperSerializeManifest(t, manifest1)

	ctx := context.Background()
	staged1, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes1})
	if err != nil {
		t.Fatalf("staging 1 failed: %v", err)
	}

	// Verify path formatting is clean and exists on disk
	if !filepath.IsAbs(staged1.ArtifactPath) {
		t.Fatalf("expected absolute path, got %s", staged1.ArtifactPath)
	}
	if _, err := os.Stat(staged1.ArtifactPath); err != nil {
		t.Fatalf("artifact not accessible on disk: %v", err)
	}

	// Test 16B: Overwriting candidate with a newer version replaces destination cleanly on Windows
	manifest2 := helperBuildValidManifest("iforest-win-2", "2.0.0", 0.65)
	rawBytes2 := helperSerializeManifest(t, manifest2)

	staged2, err := store.StageModel(ctx, StageRequest{Artifact: rawBytes2})
	if err != nil {
		t.Fatalf("staging 2 (version replacement) failed on Windows filesystem: %v", err)
	}

	if staged2.Manifest.ModelVersion != "2.0.0" {
		t.Fatalf("expected version 2.0.0, got %s", staged2.Manifest.ModelVersion)
	}

	// Test 16C: ClearCandidate removes candidate files cleanly
	if err := store.ClearCandidate(ctx); err != nil {
		t.Fatalf("ClearCandidate failed: %v", err)
	}
	_, err = store.GetCandidate(ctx)
	if !errors.Is(err, ErrNoCandidateModel) {
		t.Fatalf("expected ErrNoCandidateModel after clear, got: %v", err)
	}
}
