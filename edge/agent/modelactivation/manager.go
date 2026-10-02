package modelactivation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/detector"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/modelstore"
	"github.com/Chamalka-heshi/AegisEdge/shared/ml"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Activation and rollback domain errors.
var (
	ErrNoActiveModel             = errors.New("no active model loaded for inference")
	ErrNoCandidateModel          = errors.New("no staged candidate model found for activation")
	ErrCandidateMismatch         = errors.New("staged candidate identity does not match requested candidate ID")
	ErrActiveModelConflict       = errors.New("active model conflict: identical version with different checksum")
	ErrActivationFailed          = errors.New("model activation failed")
	ErrNoPreviousModel           = errors.New("no previous model available for rollback")
	ErrPreviousModelUnavailable  = errors.New("previous model artifact is unavailable on storage")
	ErrPreviousModelInvalid      = errors.New("previous model failed structural or integrity validation")
	ErrPreviousModelIncompatible = errors.New("previous model is incompatible with edge runtime")
	ErrRollbackFailed            = errors.New("model rollback failed")
	ErrNilModelStore             = errors.New("model store cannot be nil")
)

// ModelInfo summarizes identity, schema, and structural properties of a model artifact.
type ModelInfo struct {
	ModelID               string    `json:"model_id"`
	ModelVersion          string    `json:"model_version"`
	ArtifactFormatVersion string    `json:"artifact_format_version,omitempty"`
	FeatureSchemaVersion  string    `json:"feature_schema_version"`
	ChecksumSHA256        string    `json:"checksum_sha256"`
	DecisionThreshold     float64   `json:"decision_threshold"`
	TreeCount             int       `json:"tree_count"`
	SubSampleSize         int       `json:"sub_sample_size"`
	CreatedAt             time.Time `json:"created_at"`
}

// ActiveModelSnapshot provides an immutable point-in-time snapshot of the currently active model.
type ActiveModelSnapshot struct {
	Info        ModelInfo              `json:"info"`
	Metadata    *ml.DeploymentMetadata `json:"metadata,omitempty"`
	ActivatedAt time.Time              `json:"activated_at"`
	Manifest    *ml.ModelManifest      `json:"-"` // immutable cloned manifest
}

// PreviousModelSnapshot provides an immutable snapshot of the retained previous model.
type PreviousModelSnapshot struct {
	Info       ModelInfo              `json:"info"`
	Metadata   *ml.DeploymentMetadata `json:"metadata,omitempty"`
	RetainedAt time.Time              `json:"retained_at"`
	Manifest   *ml.ModelManifest      `json:"-"` // immutable cloned manifest
}

// CandidateSnapshot provides an immutable snapshot of the staged candidate model.
type CandidateSnapshot struct {
	Info     ModelInfo              `json:"info"`
	Metadata *ml.DeploymentMetadata `json:"metadata,omitempty"`
	StagedAt time.Time              `json:"staged_at"`
}

// ActivationResult records the outcome of an activation request.
type ActivationResult struct {
	Success        bool       `json:"success"`
	AlreadyActive  bool       `json:"already_active"`
	ModelID        string     `json:"model_id"`
	ModelVersion   string     `json:"model_version"`
	ChecksumSHA256 string     `json:"checksum_sha256"`
	ActivatedAt    time.Time  `json:"activated_at"`
	PreviousModel  *ModelInfo `json:"previous_model,omitempty"`
	FailureReason  string     `json:"failure_reason,omitempty"`
}

// RollbackResult records the outcome of an explicit rollback request.
type RollbackResult struct {
	Success       bool       `json:"success"`
	RolledBack    bool       `json:"rolled_back"`
	ActiveModel   ModelInfo  `json:"active_model"`
	PreviousModel *ModelInfo `json:"previous_model,omitempty"`
	RollbackAt    time.Time  `json:"rollback_at"`
	FailureReason string     `json:"failure_reason,omitempty"`
}

// ActivationStatus summarizes the complete runtime activation state.
type ActivationStatus struct {
	HasActiveModel       bool              `json:"has_active_model"`
	ActiveModel          *ModelInfo        `json:"active_model,omitempty"`
	ActivatedAt          *time.Time        `json:"activated_at,omitempty"`
	HasCandidate         bool              `json:"has_candidate"`
	CandidateModel       *ModelInfo        `json:"candidate_model,omitempty"`
	HasPreviousModel     bool              `json:"has_previous_model"`
	PreviousModel        *ModelInfo        `json:"previous_model,omitempty"`
	RollbackAvailable    bool              `json:"rollback_available"`
	LastActivationResult *ActivationResult `json:"last_activation_result,omitempty"`
	LastRollbackResult   *RollbackResult   `json:"last_rollback_result,omitempty"`
}

// RuntimeDetector extends detector.Detector with vector-level inference methods.
// All inference calls route to the currently active MLDetector under read lock.
type RuntimeDetector interface {
	detector.Detector

	// DetectVector evaluates a canonical feature vector against the active Isolation Forest model.
	DetectVector(ctx context.Context, nodeID string, timestamp time.Time, sampleID string, features []float64) (*types.AnomalySignal, error)

	// ComputeAnomalyScore computes raw path lengths and anomaly scores using the active model.
	ComputeAnomalyScore(features []float64) (float64, float64, error)
}

// Manager defines the contract for edge ML model activation, rollback, and runtime switching.
type Manager interface {
	// Activate validates the candidate model staged in ModelStore and promotes it to active.
	// If candidateID is non-empty, it verifies that the staged candidate matches candidateID.
	Activate(ctx context.Context, candidateID string) (*ActivationResult, error)

	// Rollback switches the currently active ML model back to the previously active known-good model.
	// The previous model is validated and a new detector constructed before runtime state changes.
	Rollback(ctx context.Context) (*RollbackResult, error)

	// GetActive returns an immutable snapshot of the active model, or ErrNoActiveModel if none is active.
	GetActive(ctx context.Context) (*ActiveModelSnapshot, error)

	// GetCandidate returns an immutable snapshot of the candidate model, or ErrNoCandidateModel if none is staged.
	GetCandidate(ctx context.Context) (*CandidateSnapshot, error)

	// GetPrevious returns an immutable snapshot of the retained previous model, or ErrNoPreviousModel if none is retained.
	GetPrevious(ctx context.Context) (*PreviousModelSnapshot, error)

	// Status returns a point-in-time summary of the runtime activation state.
	Status(ctx context.Context) ActivationStatus

	// RuntimeDetector returns a thread-safe Detector implementation that dynamically
	// evaluates inference using the currently active model.
	RuntimeDetector() RuntimeDetector
}

// ManagerConfig configures the model activation manager.
type ManagerConfig struct {
	Store            modelstore.ModelStore
	CompatibilityReq ml.CompatibilityRequirement
	InitialManifest  *ml.ModelManifest // optional startup model
	InitialMetadata  *ml.DeploymentMetadata
}

// DefaultManager implements Manager.
// Concurrency strategy:
//   - rwMu (sync.RWMutex) protects active model reference and snapshot for high-throughput concurrent readers.
//   - activationMu (sync.Mutex) serializes activation and rollback pipelines so multiple callers cannot race during promotion.
type DefaultManager struct {
	store          modelstore.ModelStore
	compatReq      ml.CompatibilityRequirement
	rwMu           sync.RWMutex
	active         *ActiveModelSnapshot
	activeDetector *detector.MLDetector
	previous       *PreviousModelSnapshot
	lastResult     *ActivationResult
	lastRollback   *RollbackResult
	activationMu   sync.Mutex
	runtimeBridge  *runtimeDetectorBridge
}

// NewManager initializes a new model activation manager.
// If cfg.InitialManifest is provided, it validates and activates it as the initial active model and persists it.
// If cfg.InitialManifest is nil, it attempts best-effort recovery of persisted active and previous models from store.
func NewManager(cfg ManagerConfig) (*DefaultManager, error) {
	if cfg.Store == nil {
		return nil, ErrNilModelStore
	}

	compatReq := cfg.CompatibilityReq
	if compatReq.SupportedAlgorithm == "" {
		compatReq = ml.DefaultCompatibilityRequirements()
	}

	mgr := &DefaultManager{
		store:     cfg.Store,
		compatReq: compatReq,
	}
	mgr.runtimeBridge = &runtimeDetectorBridge{manager: mgr}

	if cfg.InitialManifest != nil {
		if err := mgr.setInitialModel(cfg.InitialManifest, cfg.InitialMetadata); err != nil {
			return nil, fmt.Errorf("failed to initialize startup model: %w", err)
		}
	} else {
		// Best-effort restart recovery of active and previous model from local store
		_ = mgr.recoverFromStore(context.Background())
	}

	return mgr, nil
}

// recoverFromStore inspects the model store on startup to restore previously active and previous models.
func (m *DefaultManager) recoverFromStore(ctx context.Context) error {
	storedActive, err := m.store.GetActive(ctx)
	if err != nil || storedActive == nil || storedActive.Manifest == nil {
		return nil
	}

	if err := storedActive.Manifest.Validate(); err != nil {
		return nil
	}
	compatReport := ml.CheckCompatibility(storedActive.Manifest, m.compatReq)
	if !compatReport.Compatible {
		return nil
	}

	det, err := detector.NewMLDetector(detector.MLDetectorConfig{Manifest: storedActive.Manifest})
	if err != nil {
		return nil
	}

	m.active = &ActiveModelSnapshot{
		Info:        buildModelInfo(storedActive.Manifest),
		Metadata:    storedActive.Metadata,
		ActivatedAt: storedActive.StoredAt,
		Manifest:    storedActive.Manifest.Clone(),
	}
	m.activeDetector = det

	// Check if previous model exists in storage
	if storedPrev, pErr := m.store.GetPrevious(ctx); pErr == nil && storedPrev != nil && storedPrev.Manifest != nil {
		m.previous = &PreviousModelSnapshot{
			Info:       buildModelInfo(storedPrev.Manifest),
			Metadata:   storedPrev.Metadata,
			RetainedAt: storedPrev.StoredAt,
			Manifest:   storedPrev.Manifest.Clone(),
		}
	}

	return nil
}

// setInitialModel validates and sets the initial active model at startup.
func (m *DefaultManager) setInitialModel(manifest *ml.ModelManifest, metadata *ml.DeploymentMetadata) error {
	if manifest == nil {
		return ml.ErrNilModelManifest
	}
	if err := manifest.Validate(); err != nil {
		return fmt.Errorf("initial manifest validation failed: %w", err)
	}

	compatReport := ml.CheckCompatibility(manifest, m.compatReq)
	if !compatReport.Compatible {
		return fmt.Errorf("%w: %s", ErrActivationFailed, strings.Join(compatReport.Issues, "; "))
	}

	det, err := detector.NewMLDetector(detector.MLDetectorConfig{Manifest: manifest})
	if err != nil {
		return fmt.Errorf("initial detector initialization failed: %w", err)
	}

	now := time.Now().UTC()
	m.active = &ActiveModelSnapshot{
		Info:        buildModelInfo(manifest),
		Metadata:    metadata,
		ActivatedAt: now,
		Manifest:    manifest.Clone(),
	}
	m.activeDetector = det
	m.lastResult = &ActivationResult{
		Success:        true,
		AlreadyActive:  false,
		ModelID:        manifest.ModelID,
		ModelVersion:   manifest.ModelVersion,
		ChecksumSHA256: manifest.ChecksumSHA256,
		ActivatedAt:    now,
	}

	// Persist active model to store
	_, _ = m.store.SaveActive(context.Background(), manifest, metadata)

	return nil
}

// Activate promotes the currently staged candidate in ModelStore to the active runtime model.
// Execution Flow:
//  1. Acquires activationMu to serialize promotion requests.
//  2. Reads staged candidate from ModelStore.
//  3. Verifies candidate identity matches candidateID if specified.
//  4. Re-validates candidate manifest, checksum, structural integrity, and compatibility.
//  5. Evaluates idempotency and active conflict.
//  6. Constructs immutable detector.MLDetector instance.
//  7. Rotates active model to previous in ModelStore, and saves new active model.
//  8. Under rwMu write lock, swaps the active detector reference and retains previous model.
//
// Invariant: If any step fails, the existing active model remains unchanged and continues to serve subsequent inference requests.
func (m *DefaultManager) Activate(ctx context.Context, candidateID string) (*ActivationResult, error) {
	m.activationMu.Lock()
	defer m.activationMu.Unlock()

	// 1. Retrieve staged candidate from ModelStore
	staged, err := m.store.GetCandidate(ctx)
	if err != nil {
		if errors.Is(err, modelstore.ErrNoCandidateModel) {
			m.recordFailure("", "", "", ErrNoCandidateModel.Error())
			return nil, ErrNoCandidateModel
		}
		failMsg := fmt.Sprintf("failed to read staged candidate: %v", err)
		m.recordFailure("", "", "", failMsg)
		return nil, fmt.Errorf("%w: %v", ErrActivationFailed, err)
	}
	if staged == nil || staged.Manifest == nil {
		m.recordFailure("", "", "", ErrNoCandidateModel.Error())
		return nil, ErrNoCandidateModel
	}

	manifest := staged.Manifest

	// 2. Deterministic candidate identity match
	if target := strings.TrimSpace(candidateID); target != "" {
		matches := target == manifest.ModelID ||
			target == fmt.Sprintf("%s:%s", manifest.ModelID, manifest.ModelVersion) ||
			(staged.Metadata != nil && target == staged.Metadata.DeploymentID)
		if !matches {
			failMsg := fmt.Sprintf("staged candidate %s:%s does not match requested %q",
				manifest.ModelID, manifest.ModelVersion, target)
			m.recordFailure(manifest.ModelID, manifest.ModelVersion, manifest.ChecksumSHA256, failMsg)
			return nil, fmt.Errorf("%w: %s", ErrCandidateMismatch, failMsg)
		}
	}

	// 3. Activation Boundary Validation Pipeline
	// Checksum verification
	if strings.TrimSpace(manifest.ChecksumSHA256) == "" {
		failMsg := "candidate manifest missing checksum_sha256"
		m.recordFailure(manifest.ModelID, manifest.ModelVersion, "", failMsg)
		return nil, fmt.Errorf("%w: %s", ErrActivationFailed, failMsg)
	}

	computedChecksum, err := manifest.ComputeChecksum()
	if err != nil || !strings.EqualFold(manifest.ChecksumSHA256, computedChecksum) {
		failMsg := fmt.Sprintf("candidate checksum mismatch: expected %s, computed %s", manifest.ChecksumSHA256, computedChecksum)
		m.recordFailure(manifest.ModelID, manifest.ModelVersion, manifest.ChecksumSHA256, failMsg)
		return nil, fmt.Errorf("%w: %s", ErrActivationFailed, failMsg)
	}

	// Structural invariants
	if err := manifest.Validate(); err != nil {
		failMsg := fmt.Sprintf("candidate structural validation failed: %v", err)
		m.recordFailure(manifest.ModelID, manifest.ModelVersion, manifest.ChecksumSHA256, failMsg)
		return nil, fmt.Errorf("%w: %s", ErrActivationFailed, failMsg)
	}

	// Model status check
	if manifest.Status != ml.ModelStatusActive {
		failMsg := fmt.Sprintf("candidate status is %q, must be ACTIVE", manifest.Status)
		m.recordFailure(manifest.ModelID, manifest.ModelVersion, manifest.ChecksumSHA256, failMsg)
		return nil, fmt.Errorf("%w: %s", ErrActivationFailed, failMsg)
	}

	// Runtime compatibility evaluation
	compatReport := ml.CheckCompatibility(manifest, m.compatReq)
	if !compatReport.Compatible {
		failMsg := fmt.Sprintf("candidate incompatible with runtime: %s", strings.Join(compatReport.Issues, "; "))
		m.recordFailure(manifest.ModelID, manifest.ModelVersion, manifest.ChecksumSHA256, failMsg)
		return nil, fmt.Errorf("%w: %s", ErrActivationFailed, failMsg)
	}

	// Metadata validation if present
	if staged.Metadata != nil {
		if err := staged.Metadata.Validate(); err != nil {
			failMsg := fmt.Sprintf("candidate metadata invalid: %v", err)
			m.recordFailure(manifest.ModelID, manifest.ModelVersion, manifest.ChecksumSHA256, failMsg)
			return nil, fmt.Errorf("%w: %s", ErrActivationFailed, failMsg)
		}
		if !strings.EqualFold(staged.Metadata.ChecksumSHA256, manifest.ChecksumSHA256) {
			failMsg := "candidate metadata checksum does not match manifest checksum"
			m.recordFailure(manifest.ModelID, manifest.ModelVersion, manifest.ChecksumSHA256, failMsg)
			return nil, fmt.Errorf("%w: %s", ErrActivationFailed, failMsg)
		}
	}

	// 4. Idempotency & Conflict Check against Active Model
	m.rwMu.RLock()
	currentActive := m.active
	m.rwMu.RUnlock()

	if currentActive != nil {
		if currentActive.Info.ModelID == manifest.ModelID && currentActive.Info.ModelVersion == manifest.ModelVersion {
			if strings.EqualFold(currentActive.Info.ChecksumSHA256, manifest.ChecksumSHA256) {
				// Idempotent: exact same model is already active
				var prevInfo *ModelInfo
				if m.previous != nil {
					info := m.previous.Info
					prevInfo = &info
				}
				res := &ActivationResult{
					Success:        true,
					AlreadyActive:  true,
					ModelID:        currentActive.Info.ModelID,
					ModelVersion:   currentActive.Info.ModelVersion,
					ChecksumSHA256: currentActive.Info.ChecksumSHA256,
					ActivatedAt:    currentActive.ActivatedAt,
					PreviousModel:  prevInfo,
				}
				m.rwMu.Lock()
				m.lastResult = res
				m.rwMu.Unlock()
				return res, nil
			}

			// Conflict: identical version but different checksum
			failMsg := fmt.Sprintf("model %s version %s already active with checksum %s; cannot activate conflicting checksum %s",
				manifest.ModelID, manifest.ModelVersion, currentActive.Info.ChecksumSHA256, manifest.ChecksumSHA256)
			m.recordFailure(manifest.ModelID, manifest.ModelVersion, manifest.ChecksumSHA256, failMsg)
			return nil, fmt.Errorf("%w: %s", ErrActiveModelConflict, failMsg)
		}
	}

	// 5. Construct new runtime detector (verifies execution readiness before state mutation)
	newDetector, err := detector.NewMLDetector(detector.MLDetectorConfig{
		Manifest: manifest,
	})
	if err != nil {
		failMsg := fmt.Sprintf("failed to construct runtime detector: %v", err)
		m.recordFailure(manifest.ModelID, manifest.ModelVersion, manifest.ChecksumSHA256, failMsg)
		return nil, fmt.Errorf("%w: %s", ErrActivationFailed, failMsg)
	}

	// 6. Persist to ModelStore (rotate current active to previous, and save new active)
	if currentActive != nil {
		_ = m.store.RotateActiveToPrevious(ctx)
	}
	_, _ = m.store.SaveActive(ctx, manifest, staged.Metadata)

	// 7. Switch Active Model Reference under write lock
	now := time.Now().UTC()
	newSnapshot := &ActiveModelSnapshot{
		Info:        buildModelInfo(manifest),
		Metadata:    staged.Metadata,
		ActivatedAt: now,
		Manifest:    manifest.Clone(),
	}

	m.rwMu.Lock()
	if m.active != nil {
		m.previous = &PreviousModelSnapshot{
			Info:       m.active.Info,
			Metadata:   m.active.Metadata,
			RetainedAt: now,
			Manifest:   m.active.Manifest.Clone(),
		}
	}
	m.active = newSnapshot
	m.activeDetector = newDetector

	var prevInfo *ModelInfo
	if m.previous != nil {
		info := m.previous.Info
		prevInfo = &info
	}

	result := &ActivationResult{
		Success:        true,
		AlreadyActive:  false,
		ModelID:        manifest.ModelID,
		ModelVersion:   manifest.ModelVersion,
		ChecksumSHA256: manifest.ChecksumSHA256,
		ActivatedAt:    now,
		PreviousModel:  prevInfo,
	}
	m.lastResult = result
	m.rwMu.Unlock()

	return result, nil
}

// Rollback switches the currently active ML model back to the previously active known-good model.
// Execution Flow:
//  1. Acquires activationMu to serialize promotion/rollback requests.
//  2. Checks presence of previous model in memory and storage.
//  3. Loads and deserializes previous model artifact from ModelStore.
//  4. Re-validates previous manifest checksum, structural integrity, lifecycle status, and compatibility.
//  5. Evaluates idempotency (if active and previous are identical).
//  6. Constructs new runtime detector from previous model.
//  7. Swaps active and previous artifacts in ModelStore via safe local filesystem rotation.
//  8. Under rwMu write lock, swaps active and previous model references.
//
// Invariant: If any step fails, the existing active model remains unchanged and continues to serve subsequent inference requests.
// The implementation protects the logical active/previous model state during normal operation, but crash consistency of filesystem rotation
// across arbitrary process or machine failures is not formally guaranteed or verified in this phase.
func (m *DefaultManager) Rollback(ctx context.Context) (*RollbackResult, error) {
	m.activationMu.Lock()
	defer m.activationMu.Unlock()

	// 1. Inspect current active and previous models
	m.rwMu.RLock()
	currentActive := m.active
	currentPrev := m.previous
	m.rwMu.RUnlock()

	if currentPrev == nil {
		m.recordRollbackFailure("", "", "", ErrNoPreviousModel.Error())
		return nil, ErrNoPreviousModel
	}

	// 2. Retrieve previous model artifact from store
	prevStored, err := m.store.GetPrevious(ctx)
	if err != nil {
		if errors.Is(err, modelstore.ErrNoPreviousModel) {
			// If store has no previous artifact, check if in-memory manifest is available
			if currentPrev.Manifest != nil {
				prevStored = &modelstore.StoredModel{
					Manifest: currentPrev.Manifest,
					Metadata: currentPrev.Metadata,
					StoredAt: currentPrev.RetainedAt,
				}
			} else {
				m.recordRollbackFailure(currentPrev.Info.ModelID, currentPrev.Info.ModelVersion, currentPrev.Info.ChecksumSHA256, ErrPreviousModelUnavailable.Error())
				return nil, fmt.Errorf("%w: %v", ErrPreviousModelUnavailable, err)
			}
		} else {
			failMsg := fmt.Sprintf("failed to read previous model from storage: %v", err)
			m.recordRollbackFailure(currentPrev.Info.ModelID, currentPrev.Info.ModelVersion, currentPrev.Info.ChecksumSHA256, failMsg)
			return nil, fmt.Errorf("%w: %v", ErrPreviousModelUnavailable, err)
		}
	}

	if prevStored == nil || prevStored.Manifest == nil {
		m.recordRollbackFailure(currentPrev.Info.ModelID, currentPrev.Info.ModelVersion, currentPrev.Info.ChecksumSHA256, "previous model manifest is nil")
		return nil, ErrPreviousModelUnavailable
	}

	prevManifest := prevStored.Manifest

	// 3. Rollback Validation Pipeline
	// Checksum verification
	if strings.TrimSpace(prevManifest.ChecksumSHA256) == "" {
		failMsg := "previous model manifest missing checksum_sha256"
		m.recordRollbackFailure(prevManifest.ModelID, prevManifest.ModelVersion, "", failMsg)
		return nil, fmt.Errorf("%w: %s", ErrPreviousModelInvalid, failMsg)
	}

	computedSum, err := prevManifest.ComputeChecksum()
	if err != nil || !strings.EqualFold(prevManifest.ChecksumSHA256, computedSum) {
		failMsg := fmt.Sprintf("previous model checksum mismatch: expected %s, computed %s", prevManifest.ChecksumSHA256, computedSum)
		m.recordRollbackFailure(prevManifest.ModelID, prevManifest.ModelVersion, prevManifest.ChecksumSHA256, failMsg)
		return nil, fmt.Errorf("%w: %s", ErrPreviousModelInvalid, failMsg)
	}

	// Structural invariants
	if err := prevManifest.Validate(); err != nil {
		failMsg := fmt.Sprintf("previous model structural validation failed: %v", err)
		m.recordRollbackFailure(prevManifest.ModelID, prevManifest.ModelVersion, prevManifest.ChecksumSHA256, failMsg)
		return nil, fmt.Errorf("%w: %s", ErrPreviousModelInvalid, failMsg)
	}

	// Model status check
	if prevManifest.Status != ml.ModelStatusActive {
		failMsg := fmt.Sprintf("previous model status is %q, must be ACTIVE", prevManifest.Status)
		m.recordRollbackFailure(prevManifest.ModelID, prevManifest.ModelVersion, prevManifest.ChecksumSHA256, failMsg)
		return nil, fmt.Errorf("%w: %s", ErrPreviousModelInvalid, failMsg)
	}

	// Runtime compatibility evaluation
	compatReport := ml.CheckCompatibility(prevManifest, m.compatReq)
	if !compatReport.Compatible {
		failMsg := fmt.Sprintf("previous model incompatible with runtime: %s", strings.Join(compatReport.Issues, "; "))
		m.recordRollbackFailure(prevManifest.ModelID, prevManifest.ModelVersion, prevManifest.ChecksumSHA256, failMsg)
		return nil, fmt.Errorf("%w: %s", ErrPreviousModelIncompatible, failMsg)
	}

	// Metadata validation if present
	if prevStored.Metadata != nil {
		if err := prevStored.Metadata.Validate(); err != nil {
			failMsg := fmt.Sprintf("previous model metadata invalid: %v", err)
			m.recordRollbackFailure(prevManifest.ModelID, prevManifest.ModelVersion, prevManifest.ChecksumSHA256, failMsg)
			return nil, fmt.Errorf("%w: %s", ErrPreviousModelInvalid, failMsg)
		}
		if !strings.EqualFold(prevStored.Metadata.ChecksumSHA256, prevManifest.ChecksumSHA256) {
			failMsg := "previous model metadata checksum does not match manifest checksum"
			m.recordRollbackFailure(prevManifest.ModelID, prevManifest.ModelVersion, prevManifest.ChecksumSHA256, failMsg)
			return nil, fmt.Errorf("%w: %s", ErrPreviousModelInvalid, failMsg)
		}
	}

	// 4. Idempotency Check: if active model is already identical to previous
	if currentActive != nil &&
		currentActive.Info.ModelID == prevManifest.ModelID &&
		currentActive.Info.ModelVersion == prevManifest.ModelVersion &&
		strings.EqualFold(currentActive.Info.ChecksumSHA256, prevManifest.ChecksumSHA256) {
		prevInfo := currentPrev.Info
		res := &RollbackResult{
			Success:       true,
			RolledBack:    false,
			ActiveModel:   currentActive.Info,
			PreviousModel: &prevInfo,
			RollbackAt:    currentActive.ActivatedAt,
		}
		m.rwMu.Lock()
		m.lastRollback = res
		m.rwMu.Unlock()
		return res, nil
	}

	// 5. Construct new runtime detector
	newDetector, err := detector.NewMLDetector(detector.MLDetectorConfig{
		Manifest: prevManifest,
	})
	if err != nil {
		failMsg := fmt.Sprintf("failed to construct runtime detector for previous model: %v", err)
		m.recordRollbackFailure(prevManifest.ModelID, prevManifest.ModelVersion, prevManifest.ChecksumSHA256, failMsg)
		return nil, fmt.Errorf("%w: %s", ErrRollbackFailed, failMsg)
	}

	// 6. Persist swap on disk
	_ = m.store.SwapActiveAndPrevious(ctx)

	// 7. Swap active and previous in memory under write lock
	now := time.Now().UTC()
	newActiveSnapshot := &ActiveModelSnapshot{
		Info:        buildModelInfo(prevManifest),
		Metadata:    prevStored.Metadata,
		ActivatedAt: now,
		Manifest:    prevManifest.Clone(),
	}

	var newPreviousSnapshot *PreviousModelSnapshot
	if currentActive != nil {
		newPreviousSnapshot = &PreviousModelSnapshot{
			Info:       currentActive.Info,
			Metadata:   currentActive.Metadata,
			RetainedAt: now,
			Manifest:   currentActive.Manifest.Clone(),
		}
	}

	m.rwMu.Lock()
	m.active = newActiveSnapshot
	m.activeDetector = newDetector
	m.previous = newPreviousSnapshot

	var prevInfo *ModelInfo
	if newPreviousSnapshot != nil {
		info := newPreviousSnapshot.Info
		prevInfo = &info
	}

	result := &RollbackResult{
		Success:       true,
		RolledBack:    true,
		ActiveModel:   newActiveSnapshot.Info,
		PreviousModel: prevInfo,
		RollbackAt:    now,
	}
	m.lastRollback = result
	m.rwMu.Unlock()

	return result, nil
}

// GetActive returns an immutable snapshot of the active model, or ErrNoActiveModel.
func (m *DefaultManager) GetActive(ctx context.Context) (*ActiveModelSnapshot, error) {
	m.rwMu.RLock()
	defer m.rwMu.RUnlock()

	if m.active == nil {
		return nil, ErrNoActiveModel
	}

	return &ActiveModelSnapshot{
		Info:        m.active.Info,
		Metadata:    m.active.Metadata,
		ActivatedAt: m.active.ActivatedAt,
		Manifest:    m.active.Manifest.Clone(),
	}, nil
}

// GetCandidate returns an immutable snapshot of the staged candidate model, or ErrNoCandidateModel.
func (m *DefaultManager) GetCandidate(ctx context.Context) (*CandidateSnapshot, error) {
	staged, err := m.store.GetCandidate(ctx)
	if err != nil {
		if errors.Is(err, modelstore.ErrNoCandidateModel) {
			return nil, ErrNoCandidateModel
		}
		return nil, fmt.Errorf("failed to retrieve candidate from store: %w", err)
	}
	if staged == nil || staged.Manifest == nil {
		return nil, ErrNoCandidateModel
	}

	return &CandidateSnapshot{
		Info:     buildModelInfo(staged.Manifest),
		Metadata: staged.Metadata,
		StagedAt: staged.StagedAt,
	}, nil
}

// GetPrevious returns an immutable snapshot of the retained previous model, or ErrNoPreviousModel if none is retained.
func (m *DefaultManager) GetPrevious(ctx context.Context) (*PreviousModelSnapshot, error) {
	m.rwMu.RLock()
	defer m.rwMu.RUnlock()

	if m.previous == nil {
		return nil, ErrNoPreviousModel
	}

	return &PreviousModelSnapshot{
		Info:       m.previous.Info,
		Metadata:   m.previous.Metadata,
		RetainedAt: m.previous.RetainedAt,
		Manifest:   m.previous.Manifest.Clone(),
	}, nil
}

// Status returns a point-in-time snapshot of the activation state.
func (m *DefaultManager) Status(ctx context.Context) ActivationStatus {
	m.rwMu.RLock()
	defer m.rwMu.RUnlock()

	status := ActivationStatus{
		HasActiveModel:   m.active != nil,
		HasPreviousModel: m.previous != nil,
	}

	if m.active != nil {
		activeInfo := m.active.Info
		activatedAt := m.active.ActivatedAt
		status.ActiveModel = &activeInfo
		status.ActivatedAt = &activatedAt
	}

	if m.previous != nil {
		prevInfo := m.previous.Info
		status.PreviousModel = &prevInfo
		status.RollbackAvailable = true
	}

	if m.lastResult != nil {
		lastRes := *m.lastResult
		status.LastActivationResult = &lastRes
	}

	if m.lastRollback != nil {
		lastRoll := *m.lastRollback
		status.LastRollbackResult = &lastRoll
	}

	// Check candidate from store
	if cand, err := m.store.GetCandidate(ctx); err == nil && cand != nil && cand.Manifest != nil {
		status.HasCandidate = true
		candInfo := buildModelInfo(cand.Manifest)
		status.CandidateModel = &candInfo
	}

	return status
}

// RuntimeDetector returns the thread-safe dynamic detector adapter.
func (m *DefaultManager) RuntimeDetector() RuntimeDetector {
	return m.runtimeBridge
}

// recordFailure records a failed activation result for observability.
func (m *DefaultManager) recordFailure(modelID, version, checksum, reason string) {
	m.rwMu.Lock()
	defer m.rwMu.Unlock()

	var prevInfo *ModelInfo
	if m.previous != nil {
		info := m.previous.Info
		prevInfo = &info
	}

	m.lastResult = &ActivationResult{
		Success:        false,
		AlreadyActive:  false,
		ModelID:        modelID,
		ModelVersion:   version,
		ChecksumSHA256: checksum,
		ActivatedAt:    time.Now().UTC(),
		PreviousModel:  prevInfo,
		FailureReason:  reason,
	}
}

// recordRollbackFailure records a failed rollback result for observability.
func (m *DefaultManager) recordRollbackFailure(modelID, version, checksum, reason string) {
	m.rwMu.Lock()
	defer m.rwMu.Unlock()

	var prevInfo *ModelInfo
	if m.previous != nil {
		info := m.previous.Info
		prevInfo = &info
	}

	m.lastRollback = &RollbackResult{
		Success:       false,
		RolledBack:    false,
		ActiveModel:   ModelInfo{ModelID: modelID, ModelVersion: version, ChecksumSHA256: checksum},
		PreviousModel: prevInfo,
		RollbackAt:    time.Now().UTC(),
		FailureReason: reason,
	}
}

// buildModelInfo extracts ModelInfo summary attributes from a ModelManifest.
func buildModelInfo(m *ml.ModelManifest) ModelInfo {
	return ModelInfo{
		ModelID:               m.ModelID,
		ModelVersion:          m.ModelVersion,
		ArtifactFormatVersion: m.ArtifactFormatVersion,
		FeatureSchemaVersion:  m.FeatureSchemaVersion,
		ChecksumSHA256:        m.ChecksumSHA256,
		DecisionThreshold:     m.DecisionThreshold,
		TreeCount:             len(m.Trees),
		SubSampleSize:         m.SubSampleSize,
		CreatedAt:             m.CreatedAt,
	}
}

// runtimeDetectorBridge delegates detector calls dynamically to the active MLDetector.
type runtimeDetectorBridge struct {
	manager *DefaultManager
}

func (b *runtimeDetectorBridge) Name() string {
	b.manager.rwMu.RLock()
	defer b.manager.rwMu.RUnlock()
	if b.manager.activeDetector == nil {
		return "ml_isolation_forest (unloaded)"
	}
	return b.manager.activeDetector.Name()
}

func (b *runtimeDetectorBridge) Version() string {
	b.manager.rwMu.RLock()
	defer b.manager.rwMu.RUnlock()
	if b.manager.activeDetector == nil {
		return ""
	}
	return b.manager.activeDetector.Version()
}

func (b *runtimeDetectorBridge) Detect(ctx context.Context, sample types.MetricSample) (*types.AnomalySignal, error) {
	b.manager.rwMu.RLock()
	det := b.manager.activeDetector
	b.manager.rwMu.RUnlock()

	if det == nil {
		return nil, ErrNoActiveModel
	}
	return det.Detect(ctx, sample)
}

func (b *runtimeDetectorBridge) DetectBatch(ctx context.Context, batch *types.TelemetryBatch) ([]*types.AnomalySignal, error) {
	b.manager.rwMu.RLock()
	det := b.manager.activeDetector
	b.manager.rwMu.RUnlock()

	if det == nil {
		return nil, ErrNoActiveModel
	}
	return det.DetectBatch(ctx, batch)
}

func (b *runtimeDetectorBridge) DetectVector(ctx context.Context, nodeID string, timestamp time.Time, sampleID string, features []float64) (*types.AnomalySignal, error) {
	b.manager.rwMu.RLock()
	det := b.manager.activeDetector
	b.manager.rwMu.RUnlock()

	if det == nil {
		return nil, ErrNoActiveModel
	}
	return det.DetectVector(ctx, nodeID, timestamp, sampleID, features)
}

func (b *runtimeDetectorBridge) ComputeAnomalyScore(features []float64) (float64, float64, error) {
	b.manager.rwMu.RLock()
	det := b.manager.activeDetector
	b.manager.rwMu.RUnlock()

	if det == nil {
		return 0, 0, ErrNoActiveModel
	}
	norm, err := det.NormalizeFeatures(features)
	if err != nil {
		return 0, 0, err
	}
	avgPath, score, _, err := det.Score(norm)
	return avgPath, score, err
}
