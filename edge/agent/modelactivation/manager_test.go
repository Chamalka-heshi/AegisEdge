package modelactivation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/modelstore"
	"github.com/Chamalka-heshi/AegisEdge/shared/ml"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// helperBuildValidManifest creates a fully valid, structurally sound ML model manifest for testing.
func helperBuildValidManifest(id, version string, threshold float64) *ml.ModelManifest {
	manifest := &ml.ModelManifest{
		ModelID:               id,
		ModelVersion:          version,
		ArtifactFormatVersion: ml.ArtifactFormatV1,
		Algorithm:             ml.AlgorithmIsolationForest,
		FeatureSchemaVersion:  ml.FeatureSchemaV1,
		TrainingDatasetID:     "ds-activation-test",
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

func helperSerializeManifest(t *testing.T, m *ml.ModelManifest) []byte {
	t.Helper()
	data, err := ml.Serialize(m)
	if err != nil {
		t.Fatalf("helperSerializeManifest failed: %v", err)
	}
	return data
}

// -----------------------------------------------------------------------------
// 1. Startup with No Active Model
// -----------------------------------------------------------------------------

func TestModelActivation_StartupNoActiveModel(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{Store: store})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	ctx := context.Background()

	// GetActive must return ErrNoActiveModel
	active, err := mgr.GetActive(ctx)
	if !errors.Is(err, ErrNoActiveModel) {
		t.Fatalf("expected ErrNoActiveModel, got: %v", err)
	}
	if active != nil {
		t.Fatalf("expected nil active snapshot, got: %+v", active)
	}

	// Status must report no active model
	status := mgr.Status(ctx)
	if status.HasActiveModel {
		t.Fatal("expected HasActiveModel to be false")
	}
	if status.ActiveModel != nil {
		t.Fatal("expected ActiveModel to be nil")
	}

	// RuntimeDetector calls must return ErrNoActiveModel
	det := mgr.RuntimeDetector()
	if det.Name() != "ml_isolation_forest (unloaded)" {
		t.Fatalf("expected unloaded name, got: %s", det.Name())
	}
	if det.Version() != "" {
		t.Fatalf("expected empty version, got: %s", det.Version())
	}

	sample := types.MetricSample{
		NodeID:    "edge-node-01",
		Timestamp: time.Now().UTC(),
		SampleID:  "s1",
		Name:      "cpu_usage_percent",
		Value:     50.0,
	}
	if _, err := det.Detect(ctx, sample); !errors.Is(err, ErrNoActiveModel) {
		t.Fatalf("Detect should fail with ErrNoActiveModel, got: %v", err)
	}
	batch := &types.TelemetryBatch{
		BatchID:        "b-1",
		NodeID:         "edge-node-01",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{sample},
	}
	if _, err := det.DetectBatch(ctx, batch); !errors.Is(err, ErrNoActiveModel) {
		t.Fatalf("DetectBatch should fail with ErrNoActiveModel, got: %v", err)
	}
	if _, err := det.DetectVector(ctx, "edge-node-01", time.Now().UTC(), "s1", []float64{50.0, 60.0, 40.0, 50.0}); !errors.Is(err, ErrNoActiveModel) {
		t.Fatalf("DetectVector should fail with ErrNoActiveModel, got: %v", err)
	}
	if _, _, err := det.ComputeAnomalyScore([]float64{50.0, 60.0, 40.0, 50.0}); !errors.Is(err, ErrNoActiveModel) {
		t.Fatalf("ComputeAnomalyScore should fail with ErrNoActiveModel, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 2. Startup with Initial Model
// -----------------------------------------------------------------------------

func TestModelActivation_StartupWithInitialModel(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	initialManifest := helperBuildValidManifest("startup-iforest", "1.0.0", 0.55)
	mgr, err := NewManager(ManagerConfig{
		Store:           store,
		InitialManifest: initialManifest,
	})
	if err != nil {
		t.Fatalf("failed to create manager with initial manifest: %v", err)
	}

	ctx := context.Background()

	active, err := mgr.GetActive(ctx)
	if err != nil {
		t.Fatalf("GetActive failed: %v", err)
	}
	if active.Info.ModelID != "startup-iforest" {
		t.Fatalf("unexpected model ID: %s", active.Info.ModelID)
	}
	if active.Info.ModelVersion != "1.0.0" {
		t.Fatalf("unexpected model version: %s", active.Info.ModelVersion)
	}
	if active.Info.ChecksumSHA256 != initialManifest.ChecksumSHA256 {
		t.Fatalf("checksum mismatch: %s != %s", active.Info.ChecksumSHA256, initialManifest.ChecksumSHA256)
	}

	det := mgr.RuntimeDetector()
	if det.Name() != "ml_isolation_forest" {
		t.Fatalf("expected detector name 'ml_isolation_forest', got: %s", det.Name())
	}
	if det.Version() != "1.0.0" {
		t.Fatalf("expected detector version '1.0.0', got: %s", det.Version())
	}

	// Dynamic inference call succeeds
	features := []float64{50.0, 60.0, 40.0, 50.0}
	score, normScore, err := det.ComputeAnomalyScore(features)
	if err != nil {
		t.Fatalf("ComputeAnomalyScore failed: %v", err)
	}
	if score <= 0 || normScore <= 0 {
		t.Fatalf("unexpected non-positive scores: raw=%f, norm=%f", score, normScore)
	}
}

// -----------------------------------------------------------------------------
// 3. Successful Candidate Activation and Runtime Switch
// -----------------------------------------------------------------------------

func TestModelActivation_SuccessfulActivation(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	mgr, err := NewManager(ManagerConfig{Store: store})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	ctx := context.Background()

	// Stage candidate
	manifest := helperBuildValidManifest("promoted-model", "1.2.0", 0.60)
	rawBytes := helperSerializeManifest(t, manifest)
	_, err = store.StageModel(ctx, modelstore.StageRequest{Artifact: rawBytes})
	if err != nil {
		t.Fatalf("staging candidate failed: %v", err)
	}

	// Verify candidate is visible before activation
	cand, err := mgr.GetCandidate(ctx)
	if err != nil {
		t.Fatalf("GetCandidate failed: %v", err)
	}
	if cand.Info.ModelID != "promoted-model" {
		t.Fatalf("unexpected candidate model ID: %s", cand.Info.ModelID)
	}

	// Activate candidate
	res, err := mgr.Activate(ctx, "")
	if err != nil {
		t.Fatalf("activation failed: %v", err)
	}
	if !res.Success {
		t.Fatal("expected activation success")
	}
	if res.AlreadyActive {
		t.Fatal("expected AlreadyActive to be false")
	}
	if res.ModelID != "promoted-model" || res.ModelVersion != "1.2.0" {
		t.Fatalf("unexpected result: %+v", res)
	}

	// Verify active model snapshot
	active, err := mgr.GetActive(ctx)
	if err != nil {
		t.Fatalf("GetActive failed: %v", err)
	}
	if active.Info.ModelID != "promoted-model" {
		t.Fatalf("active model ID mismatch: %s", active.Info.ModelID)
	}

	// Runtime detector dynamically routes to newly activated model
	det := mgr.RuntimeDetector()
	if det.Version() != "1.2.0" {
		t.Fatalf("expected runtime detector version 1.2.0, got: %s", det.Version())
	}
}

// -----------------------------------------------------------------------------
// 4. Candidate ID Matching and Mismatch Rejection
// -----------------------------------------------------------------------------

func TestModelActivation_CandidateIDMatching(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	mgr, _ := NewManager(ManagerConfig{Store: store})
	ctx := context.Background()

	m1 := helperBuildValidManifest("model-alpha", "1.0.0", 0.60)
	_, err := store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m1)})
	if err != nil {
		t.Fatalf("stage failed: %v", err)
	}

	// 4A: Exact ModelID match
	res, err := mgr.Activate(ctx, "model-alpha")
	if err != nil || !res.Success {
		t.Fatalf("expected success with ModelID match, got err=%v", err)
	}

	// Stage next version
	m2 := helperBuildValidManifest("model-beta", "2.0.0", 0.65)
	_, err = store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m2)})
	if err != nil {
		t.Fatalf("stage failed: %v", err)
	}

	// 4B: Mismatch candidateID must be rejected
	_, err = mgr.Activate(ctx, "wrong-id")
	if !errors.Is(err, ErrCandidateMismatch) {
		t.Fatalf("expected ErrCandidateMismatch, got: %v", err)
	}

	// Verify active model was NOT changed (still model-alpha)
	active, _ := mgr.GetActive(ctx)
	if active.Info.ModelID != "model-alpha" {
		t.Fatalf("active model should remain model-alpha, got: %s", active.Info.ModelID)
	}

	// 4C: ID:Version match format succeeds
	res2, err := mgr.Activate(ctx, "model-beta:2.0.0")
	if err != nil || !res2.Success {
		t.Fatalf("expected success with ModelID:ModelVersion match, got err=%v", err)
	}

	active2, _ := mgr.GetActive(ctx)
	if active2.Info.ModelID != "model-beta" {
		t.Fatalf("active model should be model-beta, got: %s", active2.Info.ModelID)
	}
}

// -----------------------------------------------------------------------------
// 5. Idempotent Activation of Already-Active Model
// -----------------------------------------------------------------------------

func TestModelActivation_IdempotentActivation(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	manifest := helperBuildValidManifest("idempotent-model", "1.0.0", 0.50)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: manifest})
	ctx := context.Background()

	// Stage the same manifest in store
	_, err := store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, manifest)})
	if err != nil {
		t.Fatalf("staging failed: %v", err)
	}

	// Activate candidate which is identical to the active model
	res, err := mgr.Activate(ctx, "")
	if err != nil {
		t.Fatalf("expected idempotent success, got err: %v", err)
	}
	if !res.Success {
		t.Fatal("expected res.Success=true")
	}
	if !res.AlreadyActive {
		t.Fatal("expected res.AlreadyActive=true")
	}

	// ActivatedAt must match original activation timestamp
	active, _ := mgr.GetActive(ctx)
	if !active.ActivatedAt.Equal(res.ActivatedAt) {
		t.Fatalf("ActivatedAt changed during idempotent activation: %v != %v", active.ActivatedAt, res.ActivatedAt)
	}
}

// -----------------------------------------------------------------------------
// 6. Conflicting Checksum Rejection
// -----------------------------------------------------------------------------

func TestModelActivation_ConflictingChecksumRejection(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	m1 := helperBuildValidManifest("model-conflict", "1.0.0", 0.50)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: m1})
	ctx := context.Background()

	// Construct m2 with same ID and version, but different threshold -> different checksum
	m2 := helperBuildValidManifest("model-conflict", "1.0.0", 0.75)
	if m1.ChecksumSHA256 == m2.ChecksumSHA256 {
		t.Fatal("test setup error: checksums must differ")
	}

	// We bypass store candidate conflict check to test activation-level defense
	// by staging a third temporary model first, then clearing and forcing candidate directly
	// Or simpler: construct a fake store or use store's ReplaceCandidate if needed.
	// In FileSystemModelStore, StageModel checks candidate conflict, but what if candidate was staged before active?
	// Let's create manager with m1 active, then store.StageModel(m2) if base store allows different model?
	// Wait, in modelstore, StageModel rejects conflict with CANDIDATE. Here candidate is m2.
	// Since no candidate is staged yet, store.StageModel(m2) succeeds!
	_, err := store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m2)})
	if err != nil {
		t.Fatalf("staging m2 candidate failed: %v", err)
	}

	// Now try to activate m2 while m1 is active
	_, err = mgr.Activate(ctx, "")
	if err == nil {
		t.Fatal("expected ErrActiveModelConflict, got nil")
	}
	if !errors.Is(err, ErrActiveModelConflict) {
		t.Fatalf("expected ErrActiveModelConflict, got: %v", err)
	}

	// Active model MUST remain m1 with its original checksum
	active, _ := mgr.GetActive(ctx)
	if active.Info.ChecksumSHA256 != m1.ChecksumSHA256 {
		t.Fatalf("active model checksum was modified! got %s, want %s", active.Info.ChecksumSHA256, m1.ChecksumSHA256)
	}
}

// -----------------------------------------------------------------------------
// 7. Failure Safety: Candidate Failures Leave Active Model Untouched
// -----------------------------------------------------------------------------

func TestModelActivation_FailureSafety(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	initialManifest := helperBuildValidManifest("baseline-model", "1.0.0", 0.50)
	mgr, _ := NewManager(ManagerConfig{
		Store:           store,
		InitialManifest: initialManifest,
	})
	ctx := context.Background()

	// 7A: Activate when no candidate is staged
	_, err := mgr.Activate(ctx, "")
	if !errors.Is(err, ErrNoCandidateModel) {
		t.Fatalf("expected ErrNoCandidateModel, got: %v", err)
	}
	verifyActiveModelUntouched(t, mgr, initialManifest)
}

// mockStore allows injecting custom StagedCandidate objects to test activation manager edge cases
type mockStore struct {
	candidate *modelstore.StagedCandidate
	active    *modelstore.StoredModel
	previous  *modelstore.StoredModel
	err       error
}

func (m *mockStore) StageModel(ctx context.Context, req modelstore.StageRequest) (*modelstore.StagedCandidate, error) {
	return m.candidate, m.err
}

func (m *mockStore) GetCandidate(ctx context.Context) (*modelstore.StagedCandidate, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.candidate == nil {
		return nil, modelstore.ErrNoCandidateModel
	}
	return m.candidate, nil
}

func (m *mockStore) ClearCandidate(ctx context.Context) error {
	m.candidate = nil
	return nil
}

func (m *mockStore) StoreDir() string {
	return "/tmp/mock"
}

func (m *mockStore) GetActive(ctx context.Context) (*modelstore.StoredModel, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.active == nil {
		return nil, modelstore.ErrNoActiveModel
	}
	return m.active, nil
}

func (m *mockStore) GetPrevious(ctx context.Context) (*modelstore.StoredModel, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.previous == nil {
		return nil, modelstore.ErrNoPreviousModel
	}
	return m.previous, nil
}

func (m *mockStore) SaveActive(ctx context.Context, manifest *ml.ModelManifest, metadata *ml.DeploymentMetadata) (*modelstore.StoredModel, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.active = &modelstore.StoredModel{
		Manifest: manifest,
		Metadata: metadata,
		StoredAt: time.Now().UTC(),
	}
	return m.active, nil
}

func (m *mockStore) RotateActiveToPrevious(ctx context.Context) error {
	if m.err != nil {
		return m.err
	}
	m.previous = m.active
	return nil
}

func (m *mockStore) SwapActiveAndPrevious(ctx context.Context) error {
	if m.err != nil {
		return m.err
	}
	if m.active == nil {
		return modelstore.ErrNoActiveModel
	}
	if m.previous == nil {
		return modelstore.ErrNoPreviousModel
	}
	m.active, m.previous = m.previous, m.active
	return nil
}

func (m *mockStore) ClearPrevious(ctx context.Context) error {
	m.previous = nil
	return nil
}

func verifyActiveModelUntouched(t *testing.T, mgr Manager, expected *ml.ModelManifest) {
	t.Helper()
	active, err := mgr.GetActive(context.Background())
	if err != nil {
		t.Fatalf("active model missing: %v", err)
	}
	if active.Info.ModelID != expected.ModelID {
		t.Fatalf("active model ID changed: %s != %s", active.Info.ModelID, expected.ModelID)
	}
	if active.Info.ChecksumSHA256 != expected.ChecksumSHA256 {
		t.Fatalf("active model checksum changed: %s != %s", active.Info.ChecksumSHA256, expected.ChecksumSHA256)
	}
}

func TestModelActivation_FailurePipeline_Comprehensive(t *testing.T) {
	initial := helperBuildValidManifest("active-stable", "1.0.0", 0.50)

	testCases := []struct {
		name          string
		setupCand     func() *ml.ModelManifest
		setupMeta     func() *ml.DeploymentMetadata
		storeErr      error
		expectedError error
	}{
		{
			name: "store read error",
			setupCand: func() *ml.ModelManifest {
				return nil
			},
			storeErr:      errors.New("disk I/O failure"),
			expectedError: ErrActivationFailed,
		},
		{
			name: "missing checksum",
			setupCand: func() *ml.ModelManifest {
				m := helperBuildValidManifest("cand-no-sum", "2.0.0", 0.60)
				m.ChecksumSHA256 = ""
				return m
			},
			expectedError: ErrActivationFailed,
		},
		{
			name: "checksum mismatch",
			setupCand: func() *ml.ModelManifest {
				m := helperBuildValidManifest("cand-bad-sum", "2.0.0", 0.60)
				m.ChecksumSHA256 = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
				return m
			},
			expectedError: ErrActivationFailed,
		},
		{
			name: "structural validation failure - negative threshold",
			setupCand: func() *ml.ModelManifest {
				m := helperBuildValidManifest("cand-bad-thresh", "2.0.0", -0.50)
				sum, _ := m.ComputeChecksum()
				m.ChecksumSHA256 = sum
				return m
			},
			expectedError: ErrActivationFailed,
		},
		{
			name: "status not ACTIVE (DISABLED)",
			setupCand: func() *ml.ModelManifest {
				m := helperBuildValidManifest("cand-disabled", "2.0.0", 0.60)
				m.Status = ml.ModelStatusDisabled
				sum, _ := m.ComputeChecksum()
				m.ChecksumSHA256 = sum
				return m
			},
			expectedError: ErrActivationFailed,
		},
		{
			name: "incompatible algorithm",
			setupCand: func() *ml.ModelManifest {
				m := helperBuildValidManifest("cand-incompat-algo", "2.0.0", 0.60)
				m.Algorithm = "unsupported_algo"
				sum, _ := m.ComputeChecksum()
				m.ChecksumSHA256 = sum
				return m
			},
			expectedError: ErrActivationFailed,
		},
		{
			name: "metadata checksum mismatch",
			setupCand: func() *ml.ModelManifest {
				return helperBuildValidManifest("cand-meta-mismatch", "2.0.0", 0.60)
			},
			setupMeta: func() *ml.DeploymentMetadata {
				return &ml.DeploymentMetadata{
					DeploymentID:   "dep-1",
					ModelID:        "cand-meta-mismatch",
					ModelVersion:   "2.0.0",
					ChecksumSHA256: "mismatched_checksum",
					DeployedAt:     time.Now().UTC(),
				}
			},
			expectedError: ErrActivationFailed,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockStore{
				err: tc.storeErr,
			}
			if tc.setupCand != nil {
				candManifest := tc.setupCand()
				if candManifest != nil {
					var meta *ml.DeploymentMetadata
					if tc.setupMeta != nil {
						meta = tc.setupMeta()
					}
					mock.candidate = &modelstore.StagedCandidate{
						Manifest: candManifest,
						Metadata: meta,
						StagedAt: time.Now().UTC(),
					}
				}
			}

			mgr, err := NewManager(ManagerConfig{
				Store:           mock,
				InitialManifest: initial,
			})
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}

			// Perform activation
			_, actErr := mgr.Activate(context.Background(), "")
			if actErr == nil {
				t.Fatal("expected activation failure, got nil")
			}
			if !errors.Is(actErr, tc.expectedError) {
				t.Fatalf("expected error %v, got %v", tc.expectedError, actErr)
			}

			// Invariant check: Active model must remain completely untouched
			verifyActiveModelUntouched(t, mgr, initial)

			// Runtime detector must continue serving inference with initial model
			det := mgr.RuntimeDetector()
			score, _, err := det.ComputeAnomalyScore([]float64{50.0, 60.0, 40.0, 50.0})
			if err != nil {
				t.Fatalf("inference broken after failed activation: %v", err)
			}
			if score <= 0 {
				t.Fatalf("unexpected invalid inference score: %f", score)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// 8. Inference Equivalence and Runtime Model Transition
// -----------------------------------------------------------------------------

func TestModelActivation_InferenceTransitionAndEquivalence(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	// Model 1 has threshold 0.40
	m1 := helperBuildValidManifest("model-trans", "1.0.0", 0.40)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: m1})
	ctx := context.Background()

	testVector := []float64{95.0, 95.0, 95.0, 95.0} // anomalous vector
	now := time.Now().UTC()

	det := mgr.RuntimeDetector()

	// Initial inference with Model 1
	path1, rawScore1, err := det.ComputeAnomalyScore(testVector)
	if err != nil {
		t.Fatalf("model 1 compute score failed: %v", err)
	}

	sig1, err := det.DetectVector(ctx, "node-1", now, "s1", testVector)
	if err != nil {
		t.Fatalf("model 1 inference failed: %v", err)
	}
	if sig1 == nil {
		t.Fatal("expected anomaly signal for Model 1 (threshold 0.40)")
	}

	// Stage and activate Model 2 with threshold 0.99 (should suppress signal for same score)
	m2 := helperBuildValidManifest("model-trans", "2.0.0", 0.99)
	_, err = store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m2)})
	if err != nil {
		t.Fatalf("stage m2 failed: %v", err)
	}

	actRes, err := mgr.Activate(ctx, "")
	if err != nil || !actRes.Success {
		t.Fatalf("activate m2 failed: %v", err)
	}

	// Dynamic detector now executes Model 2: signal is suppressed because rawScore < 0.99
	sig2, err := det.DetectVector(ctx, "node-1", now, "s2", testVector)
	if err != nil {
		t.Fatalf("model 2 inference failed: %v", err)
	}
	if sig2 != nil {
		t.Fatalf("expected nil signal with threshold 0.99, got signal with score %f", sig2.AnomalyScore)
	}

	// Verify raw score and path computation remain identical for identical tree structure
	path2, rawScore2, err := det.ComputeAnomalyScore(testVector)
	if err != nil {
		t.Fatalf("compute score failed: %v", err)
	}
	if rawScore1 != rawScore2 {
		t.Fatalf("raw score changed across models with identical trees: %f != %f", rawScore1, rawScore2)
	}
	if path1 != path2 {
		t.Fatalf("path length changed across models with identical trees: %f != %f", path1, path2)
	}
}

// -----------------------------------------------------------------------------
// 9. Previous Model Metadata Tracking
// -----------------------------------------------------------------------------

func TestModelActivation_PreviousModelTracking(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	m1 := helperBuildValidManifest("track-model", "1.0.0", 0.50)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: m1})
	ctx := context.Background()

	// Initial status has no previous model
	st1 := mgr.Status(ctx)
	if st1.HasPreviousModel || st1.PreviousModel != nil {
		t.Fatal("expected no previous model initially")
	}

	// Activate Model 2
	m2 := helperBuildValidManifest("track-model", "2.0.0", 0.60)
	_, _ = store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m2)})
	res2, err := mgr.Activate(ctx, "")
	if err != nil {
		t.Fatalf("activate m2 failed: %v", err)
	}

	if res2.PreviousModel == nil || res2.PreviousModel.ModelVersion != "1.0.0" {
		t.Fatalf("expected previous model 1.0.0 in res2, got: %+v", res2.PreviousModel)
	}

	st2 := mgr.Status(ctx)
	if !st2.HasPreviousModel || st2.PreviousModel.ModelVersion != "1.0.0" {
		t.Fatalf("status previous model expected 1.0.0, got: %+v", st2.PreviousModel)
	}

	// Activate Model 3
	m3 := helperBuildValidManifest("track-model", "3.0.0", 0.70)
	_, _ = store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m3)})
	res3, err := mgr.Activate(ctx, "")
	if err != nil {
		t.Fatalf("activate m3 failed: %v", err)
	}

	if res3.PreviousModel == nil || res3.PreviousModel.ModelVersion != "2.0.0" {
		t.Fatalf("expected previous model 2.0.0 in res3, got: %+v", res3.PreviousModel)
	}

	st3 := mgr.Status(ctx)
	if !st3.HasPreviousModel || st3.PreviousModel.ModelVersion != "2.0.0" {
		t.Fatalf("status previous model expected 2.0.0, got: %+v", st3.PreviousModel)
	}
}

// -----------------------------------------------------------------------------
// 10. Concurrency: Continuous Inference During Dynamic Activation
// -----------------------------------------------------------------------------

func TestModelActivation_ConcurrentInferenceAndActivation(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	m1 := helperBuildValidManifest("conc-model", "1.0.0", 0.50)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: m1})
	ctx := context.Background()

	det := mgr.RuntimeDetector()
	testVector := []float64{50.0, 60.0, 40.0, 50.0}

	const numReaders = 8
	const readerOps = 500
	var wg sync.WaitGroup
	var readErrors atomic.Int64
	var successfulReads atomic.Int64

	stopCh := make(chan struct{})

	// Start continuous inference workers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < readerOps; j++ {
				select {
				case <-stopCh:
					return
				default:
				}

				score, normScore, err := det.ComputeAnomalyScore(testVector)
				if err != nil {
					readErrors.Add(1)
				} else if score > 0 && normScore > 0 {
					successfulReads.Add(1)
				}
				time.Sleep(100 * time.Microsecond)
			}
		}(i)
	}

	// Perform 3 sequential activations while readers are executing
	for v := 2; v <= 4; v++ {
		time.Sleep(5 * time.Millisecond)
		version := fmt.Sprintf("%d.0.0", v)
		cand := helperBuildValidManifest("conc-model", version, 0.50+float64(v)*0.05)
		_, err := store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, cand)})
		if err != nil {
			t.Fatalf("staging v%d failed: %v", v, err)
		}

		res, err := mgr.Activate(ctx, "")
		if err != nil {
			t.Fatalf("activating v%d failed: %v", v, err)
		}
		if !res.Success {
			t.Fatalf("activation v%d reported not successful", v)
		}
	}

	wg.Wait()
	close(stopCh)

	if errs := readErrors.Load(); errs > 0 {
		t.Fatalf("encountered %d inference errors during concurrent activations", errs)
	}
	if reads := successfulReads.Load(); reads == 0 {
		t.Fatal("expected positive number of successful reads")
	}

	// Verify final active model version
	active, err := mgr.GetActive(ctx)
	if err != nil {
		t.Fatalf("GetActive failed: %v", err)
	}
	if active.Info.ModelVersion != "4.0.0" {
		t.Fatalf("expected final active model version 4.0.0, got: %s", active.Info.ModelVersion)
	}
}

// -----------------------------------------------------------------------------
// 11. Concurrency: Simultaneous Activation Requests
// -----------------------------------------------------------------------------

func TestModelActivation_SimultaneousActivationRequests(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	m1 := helperBuildValidManifest("race-model", "1.0.0", 0.50)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: m1})
	ctx := context.Background()

	// Stage candidate m2
	m2 := helperBuildValidManifest("race-model", "2.0.0", 0.60)
	_, err := store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m2)})
	if err != nil {
		t.Fatalf("stage failed: %v", err)
	}

	const numCallers = 10
	var wg sync.WaitGroup
	results := make([]*ActivationResult, numCallers)
	errs := make([]error, numCallers)

	for i := 0; i < numCallers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = mgr.Activate(ctx, "")
		}(i)
	}

	wg.Wait()

	// Every caller must succeed without error (either winning the promotion or reporting AlreadyActive)
	var initialPromotions int
	var idempotentReactivations int

	for i := 0; i < numCallers; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d failed with error: %v", i, errs[i])
		}
		if results[i] == nil || !results[i].Success {
			t.Fatalf("caller %d result not successful: %+v", i, results[i])
		}
		if results[i].AlreadyActive {
			idempotentReactivations++
		} else {
			initialPromotions++
		}
	}

	// Exactly 1 caller performed the initial promotion; the other 9 saw it was already active
	if initialPromotions != 1 {
		t.Fatalf("expected exactly 1 promotion, got %d", initialPromotions)
	}
	if idempotentReactivations != numCallers-1 {
		t.Fatalf("expected %d idempotent reactivations, got %d", numCallers-1, idempotentReactivations)
	}
}

// -----------------------------------------------------------------------------
// 12. Concurrency: Failed Activation During Continuous Inference
// -----------------------------------------------------------------------------

func TestModelActivation_FailedActivationDuringContinuousInference(t *testing.T) {
	initial := helperBuildValidManifest("resilient-model", "1.0.0", 0.50)
	mock := &mockStore{
		candidate: &modelstore.StagedCandidate{
			Manifest: &ml.ModelManifest{
				ModelID:        "corrupted-candidate",
				ChecksumSHA256: "bad-checksum", // will fail checksum validation
			},
		},
	}

	mgr, _ := NewManager(ManagerConfig{
		Store:           mock,
		InitialManifest: initial,
	})

	ctx := context.Background()
	det := mgr.RuntimeDetector()
	testVector := []float64{50.0, 60.0, 40.0, 50.0}

	const ops = 300
	var wg sync.WaitGroup
	var inferenceFailures atomic.Int64

	// Inference worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < ops; i++ {
			score, _, err := det.ComputeAnomalyScore(testVector)
			if err != nil || score <= 0 {
				inferenceFailures.Add(1)
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()

	// Attacker worker repeatedly triggering failed activations
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_, _ = mgr.Activate(ctx, "")
			time.Sleep(500 * time.Microsecond)
		}
	}()

	wg.Wait()

	if fails := inferenceFailures.Load(); fails > 0 {
		t.Fatalf("inference suffered %d failures during failed activations", fails)
	}

	// Active model must remain resilient-model 1.0.0
	active, _ := mgr.GetActive(ctx)
	if active.Info.ModelID != "resilient-model" {
		t.Fatalf("unexpected active model ID: %s", active.Info.ModelID)
	}
}

// -----------------------------------------------------------------------------
// 13. Rollback: No Previous Model Returns ErrNoPreviousModel
// -----------------------------------------------------------------------------

func TestModelActivation_Rollback_NoPreviousModel(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	initial := helperBuildValidManifest("solo-model", "1.0.0", 0.50)
	mgr, err := NewManager(ManagerConfig{
		Store:           store,
		InitialManifest: initial,
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	ctx := context.Background()

	// Initial status has no previous model
	status := mgr.Status(ctx)
	if status.HasPreviousModel {
		t.Fatal("expected HasPreviousModel=false")
	}

	// Rollback must fail with ErrNoPreviousModel
	res, err := mgr.Rollback(ctx)
	if !errors.Is(err, ErrNoPreviousModel) {
		t.Fatalf("expected ErrNoPreviousModel, got: %v", err)
	}
	if res != nil {
		t.Fatalf("expected nil result on failure, got: %+v", res)
	}

	// Active model remains solo-model
	verifyActiveModelUntouched(t, mgr, initial)
}

// -----------------------------------------------------------------------------
// 14. Rollback: Successful State Transition and Active/Previous Swap
// -----------------------------------------------------------------------------

func TestModelActivation_Rollback_SuccessAndSwap(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	m1 := helperBuildValidManifest("swap-model", "1.0.0", 0.40)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: m1})
	ctx := context.Background()

	// Activate Model 2
	m2 := helperBuildValidManifest("swap-model", "2.0.0", 0.70)
	_, err := store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m2)})
	if err != nil {
		t.Fatalf("stage m2 failed: %v", err)
	}

	actRes, err := mgr.Activate(ctx, "")
	if err != nil || !actRes.Success {
		t.Fatalf("activate m2 failed: %v", err)
	}

	// Status before rollback: Active=2.0.0, Previous=1.0.0
	stBefore := mgr.Status(ctx)
	if !stBefore.HasPreviousModel || !stBefore.RollbackAvailable {
		t.Fatalf("expected rollback available, got: %+v", stBefore)
	}
	if stBefore.ActiveModel.ModelVersion != "2.0.0" {
		t.Fatalf("active model version expected 2.0.0, got: %s", stBefore.ActiveModel.ModelVersion)
	}
	if stBefore.PreviousModel.ModelVersion != "1.0.0" {
		t.Fatalf("previous model version expected 1.0.0, got: %s", stBefore.PreviousModel.ModelVersion)
	}

	// Execute Rollback
	rbRes, err := mgr.Rollback(ctx)
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	if !rbRes.Success || !rbRes.RolledBack {
		t.Fatalf("expected successful rollback, got: %+v", rbRes)
	}
	if rbRes.ActiveModel.ModelVersion != "1.0.0" {
		t.Fatalf("new active model expected 1.0.0, got: %s", rbRes.ActiveModel.ModelVersion)
	}
	if rbRes.PreviousModel == nil || rbRes.PreviousModel.ModelVersion != "2.0.0" {
		t.Fatalf("previous model expected 2.0.0 after rollback, got: %+v", rbRes.PreviousModel)
	}

	// Verify runtime detector dynamically points to 1.0.0
	det := mgr.RuntimeDetector()
	if det.Version() != "1.0.0" {
		t.Fatalf("runtime detector expected version 1.0.0, got: %s", det.Version())
	}

	// Verify GetPrevious returns 2.0.0 snapshot
	prevSnap, err := mgr.GetPrevious(ctx)
	if err != nil {
		t.Fatalf("GetPrevious failed: %v", err)
	}
	if prevSnap.Info.ModelVersion != "2.0.0" {
		t.Fatalf("GetPrevious snapshot expected 2.0.0, got: %s", prevSnap.Info.ModelVersion)
	}
}

// -----------------------------------------------------------------------------
// 15. Rollback: Repeated Valid Swaps (A -> B -> A -> B)
// -----------------------------------------------------------------------------

func TestModelActivation_Rollback_RepeatedSwaps(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	mA := helperBuildValidManifest("multi-swap", "1.0.0", 0.50)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: mA})
	ctx := context.Background()

	mB := helperBuildValidManifest("multi-swap", "2.0.0", 0.60)
	_, _ = store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, mB)})
	_, _ = mgr.Activate(ctx, "")

	// 1st Rollback: B -> A
	r1, err := mgr.Rollback(ctx)
	if err != nil || !r1.Success || r1.ActiveModel.ModelVersion != "1.0.0" {
		t.Fatalf("rollback 1 failed: %v, %+v", err, r1)
	}

	// 2nd Rollback: A -> B
	r2, err := mgr.Rollback(ctx)
	if err != nil || !r2.Success || r2.ActiveModel.ModelVersion != "2.0.0" {
		t.Fatalf("rollback 2 failed: %v, %+v", err, r2)
	}

	// 3rd Rollback: B -> A
	r3, err := mgr.Rollback(ctx)
	if err != nil || !r3.Success || r3.ActiveModel.ModelVersion != "1.0.0" {
		t.Fatalf("rollback 3 failed: %v, %+v", err, r3)
	}

	if mgr.RuntimeDetector().Version() != "1.0.0" {
		t.Fatalf("detector version expected 1.0.0, got: %s", mgr.RuntimeDetector().Version())
	}
}

// -----------------------------------------------------------------------------
// 16. Rollback: Previous Artifact Missing on Storage
// -----------------------------------------------------------------------------

func TestModelActivation_Rollback_ArtifactMissingOnStorage(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})

	m1 := helperBuildValidManifest("missing-art", "1.0.0", 0.50)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: m1})
	ctx := context.Background()

	m2 := helperBuildValidManifest("missing-art", "2.0.0", 0.60)
	_, _ = store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m2)})
	_, _ = mgr.Activate(ctx, "")

	// Now delete previous model from disk and clear in-memory manifest to simulate missing storage
	_ = store.ClearPrevious(ctx)
	mgr.previous.Manifest = nil // force disk read failure

	// Rollback must fail with ErrPreviousModelUnavailable
	_, err := mgr.Rollback(ctx)
	if !errors.Is(err, ErrPreviousModelUnavailable) {
		t.Fatalf("expected ErrPreviousModelUnavailable, got: %v", err)
	}

	// Active model MUST remain 2.0.0
	active, _ := mgr.GetActive(ctx)
	if active.Info.ModelVersion != "2.0.0" {
		t.Fatalf("active model was modified! expected 2.0.0, got: %s", active.Info.ModelVersion)
	}
}

// -----------------------------------------------------------------------------
// 17. Rollback: Failure Pipeline Comprehensive (Active Untouched)
// -----------------------------------------------------------------------------

func TestModelActivation_Rollback_FailurePipeline(t *testing.T) {
	activeManifest := helperBuildValidManifest("active-guardian", "2.0.0", 0.60)

	testCases := []struct {
		name          string
		setupPrev     func() *ml.ModelManifest
		setupMeta     func() *ml.DeploymentMetadata
		expectedError error
	}{
		{
			name: "previous missing checksum",
			setupPrev: func() *ml.ModelManifest {
				m := helperBuildValidManifest("active-guardian", "1.0.0", 0.50)
				m.ChecksumSHA256 = ""
				return m
			},
			expectedError: ErrPreviousModelInvalid,
		},
		{
			name: "previous checksum mismatch",
			setupPrev: func() *ml.ModelManifest {
				m := helperBuildValidManifest("active-guardian", "1.0.0", 0.50)
				m.ChecksumSHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
				return m
			},
			expectedError: ErrPreviousModelInvalid,
		},
		{
			name: "previous negative threshold",
			setupPrev: func() *ml.ModelManifest {
				m := helperBuildValidManifest("active-guardian", "1.0.0", -0.50)
				sum, _ := m.ComputeChecksum()
				m.ChecksumSHA256 = sum
				return m
			},
			expectedError: ErrPreviousModelInvalid,
		},
		{
			name: "previous status DISABLED",
			setupPrev: func() *ml.ModelManifest {
				m := helperBuildValidManifest("active-guardian", "1.0.0", 0.50)
				m.Status = ml.ModelStatusDisabled
				sum, _ := m.ComputeChecksum()
				m.ChecksumSHA256 = sum
				return m
			},
			expectedError: ErrPreviousModelInvalid,
		},
		{
			name: "previous incompatible artifact format",
			setupPrev: func() *ml.ModelManifest {
				m := helperBuildValidManifest("active-guardian", "1.0.0", 0.50)
				m.ArtifactFormatVersion = "aegisedge.model.v99"
				sum, _ := m.ComputeChecksum()
				m.ChecksumSHA256 = sum
				return m
			},
			expectedError: ErrPreviousModelIncompatible,
		},
		{
			name: "previous metadata digest mismatch",
			setupPrev: func() *ml.ModelManifest {
				return helperBuildValidManifest("active-guardian", "1.0.0", 0.50)
			},
			setupMeta: func() *ml.DeploymentMetadata {
				return &ml.DeploymentMetadata{
					DeploymentID:   "dep-bad-meta",
					ModelID:        "active-guardian",
					ModelVersion:   "1.0.0",
					ChecksumSHA256: "bad_checksum",
					DeployedAt:     time.Now().UTC(),
				}
			},
			expectedError: ErrPreviousModelInvalid,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prevManifest := tc.setupPrev()
			var prevMeta *ml.DeploymentMetadata
			if tc.setupMeta != nil {
				prevMeta = tc.setupMeta()
			}

			mock := &mockStore{
				active: &modelstore.StoredModel{
					Manifest: activeManifest,
					StoredAt: time.Now().UTC(),
				},
				previous: &modelstore.StoredModel{
					Manifest: prevManifest,
					Metadata: prevMeta,
					StoredAt: time.Now().UTC(),
				},
			}

			mgr, err := NewManager(ManagerConfig{
				Store:           mock,
				InitialManifest: activeManifest,
			})
			if err != nil {
				t.Fatalf("NewManager failed: %v", err)
			}

			// In-memory previous snapshot matches test previous
			mgr.previous = &PreviousModelSnapshot{
				Info:       buildModelInfo(prevManifest),
				Metadata:   prevMeta,
				RetainedAt: time.Now().UTC(),
				Manifest:   prevManifest,
			}

			// Attempt rollback
			_, rbErr := mgr.Rollback(context.Background())
			if rbErr == nil {
				t.Fatal("expected rollback failure, got nil")
			}
			if !errors.Is(rbErr, tc.expectedError) {
				t.Fatalf("expected error %v, got %v", tc.expectedError, rbErr)
			}

			// Active model MUST remain completely untouched!
			verifyActiveModelUntouched(t, mgr, activeManifest)

			// Runtime detector continues serving inference with active model
			det := mgr.RuntimeDetector()
			if det.Version() != "2.0.0" {
				t.Fatalf("detector version changed: %s", det.Version())
			}
		})
	}
}

// -----------------------------------------------------------------------------
// 18. Rollback: Restart Recovery from Local Storage
// -----------------------------------------------------------------------------

func TestModelActivation_Rollback_RestartRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})
	ctx := context.Background()

	// Agent 1: starts with v1, activates v2
	m1 := helperBuildValidManifest("restart-model", "1.0.0", 0.50)
	mgr1, _ := NewManager(ManagerConfig{Store: store, InitialManifest: m1})

	m2 := helperBuildValidManifest("restart-model", "2.0.0", 0.60)
	_, _ = store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m2)})
	_, _ = mgr1.Activate(ctx, "")

	// "Simulate agent restart" by initializing a new manager against the same store with InitialManifest: nil
	mgr2, err := NewManager(ManagerConfig{Store: store, InitialManifest: nil})
	if err != nil {
		t.Fatalf("mgr2 NewManager restart recovery failed: %v", err)
	}

	st := mgr2.Status(ctx)
	if !st.HasActiveModel || st.ActiveModel.ModelVersion != "2.0.0" {
		t.Fatalf("mgr2 active model expected 2.0.0, got: %+v", st.ActiveModel)
	}
	if !st.HasPreviousModel || !st.RollbackAvailable || st.PreviousModel.ModelVersion != "1.0.0" {
		t.Fatalf("mgr2 previous model expected 1.0.0, got: %+v", st.PreviousModel)
	}

	// Executing rollback on recovered agent returns to 1.0.0
	rbRes, err := mgr2.Rollback(ctx)
	if err != nil || !rbRes.Success || rbRes.ActiveModel.ModelVersion != "1.0.0" {
		t.Fatalf("mgr2 rollback failed: %v, %+v", err, rbRes)
	}

	if mgr2.RuntimeDetector().Version() != "1.0.0" {
		t.Fatalf("runtime detector expected 1.0.0, got: %s", mgr2.RuntimeDetector().Version())
	}
}

// -----------------------------------------------------------------------------
// 19. Rollback: Activation After Rollback & Rollback After Activation
// -----------------------------------------------------------------------------

func TestModelActivation_Rollback_ActivationLifecycleInterleaving(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})
	ctx := context.Background()

	// Initial: v1 active
	m1 := helperBuildValidManifest("interleave", "1.0.0", 0.50)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: m1})

	// Activate v2 -> Active: v2, Previous: v1
	m2 := helperBuildValidManifest("interleave", "2.0.0", 0.60)
	_, _ = store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m2)})
	_, _ = mgr.Activate(ctx, "")

	// Rollback -> Active: v1, Previous: v2
	rb, err := mgr.Rollback(ctx)
	if err != nil || rb.ActiveModel.ModelVersion != "1.0.0" {
		t.Fatalf("rollback to v1 failed: %v", err)
	}

	// Stage and Activate v3 -> Active: v3, Previous: v1
	m3 := helperBuildValidManifest("interleave", "3.0.0", 0.70)
	_, _ = store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m3)})
	act, err := mgr.Activate(ctx, "")
	if err != nil || act.ModelVersion != "3.0.0" {
		t.Fatalf("activate v3 failed: %v", err)
	}

	// Rollback -> Active: v1, Previous: v3
	rb2, err := mgr.Rollback(ctx)
	if err != nil || rb2.ActiveModel.ModelVersion != "1.0.0" {
		t.Fatalf("rollback to v1 after v3 failed: %v", err)
	}
	if rb2.PreviousModel.ModelVersion != "3.0.0" {
		t.Fatalf("previous model expected 3.0.0, got: %s", rb2.PreviousModel.ModelVersion)
	}
}

// -----------------------------------------------------------------------------
// 20. Concurrency: Continuous Inference During Dynamic Rollback
// -----------------------------------------------------------------------------

func TestModelActivation_Rollback_ConcurrentInference(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})
	ctx := context.Background()

	m1 := helperBuildValidManifest("conc-rb", "1.0.0", 0.50)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: m1})

	m2 := helperBuildValidManifest("conc-rb", "2.0.0", 0.60)
	_, _ = store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m2)})
	_, _ = mgr.Activate(ctx, "")

	det := mgr.RuntimeDetector()
	testVector := []float64{50.0, 60.0, 40.0, 50.0}

	const numReaders = 8
	const readerOps = 400
	var wg sync.WaitGroup
	var readErrors atomic.Int64
	var successfulReads atomic.Int64

	stopCh := make(chan struct{})

	// Continuous readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < readerOps; j++ {
				select {
				case <-stopCh:
					return
				default:
				}
				score, normScore, err := det.ComputeAnomalyScore(testVector)
				if err != nil {
					readErrors.Add(1)
				} else if score > 0 && normScore > 0 {
					successfulReads.Add(1)
				}
				time.Sleep(100 * time.Microsecond)
			}
		}()
	}

	// Trigger 4 back-and-forth rollbacks concurrently
	for r := 0; r < 4; r++ {
		time.Sleep(5 * time.Millisecond)
		res, err := mgr.Rollback(ctx)
		if err != nil || !res.Success {
			t.Fatalf("concurrent rollback %d failed: %v", r, err)
		}
	}

	wg.Wait()
	close(stopCh)

	if errs := readErrors.Load(); errs > 0 {
		t.Fatalf("encountered %d inference errors during concurrent rollbacks", errs)
	}
	if reads := successfulReads.Load(); reads == 0 {
		t.Fatal("expected positive number of successful inference evaluations")
	}

	// Final active model after 4 rollbacks (even number) returns to 2.0.0
	active, _ := mgr.GetActive(ctx)
	if active.Info.ModelVersion != "2.0.0" {
		t.Fatalf("expected final active version 2.0.0, got: %s", active.Info.ModelVersion)
	}
}

// -----------------------------------------------------------------------------
// 21. Concurrency: Simultaneous Rollback Requests
// -----------------------------------------------------------------------------

func TestModelActivation_Rollback_SimultaneousRequests(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := modelstore.NewFileSystemModelStore(modelstore.StoreConfig{BaseDir: tmpDir})
	ctx := context.Background()

	m1 := helperBuildValidManifest("sim-rb", "1.0.0", 0.50)
	mgr, _ := NewManager(ManagerConfig{Store: store, InitialManifest: m1})

	m2 := helperBuildValidManifest("sim-rb", "2.0.0", 0.60)
	_, _ = store.StageModel(ctx, modelstore.StageRequest{Artifact: helperSerializeManifest(t, m2)})
	_, _ = mgr.Activate(ctx, "")

	const callers = 10
	var wg sync.WaitGroup
	errs := make([]error, callers)
	results := make([]*RollbackResult, callers)

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = mgr.Rollback(ctx)
		}(i)
	}

	wg.Wait()

	// Every caller must execute cleanly without panic or corruption
	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d failed: %v", i, errs[i])
		}
		if results[i] == nil || !results[i].Success {
			t.Fatalf("caller %d result not successful: %+v", i, results[i])
		}
	}

	// Final state is coherent
	status := mgr.Status(ctx)
	if !status.HasActiveModel || !status.HasPreviousModel {
		t.Fatalf("incoherent status after simultaneous rollbacks: %+v", status)
	}
}
