package modelstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/ml"
)

// Default bounds and subdirectory names.
const (
	// DefaultMaxArtifactSizeBytes limits the maximum model artifact file size to 16 MiB.
	// This bounds allocations and defends against denial-of-service memory exhaustion.
	DefaultMaxArtifactSizeBytes = 16 * 1024 * 1024

	// Subdirectory names within the model store.
	DirCandidate = "candidate"
	DirStaging   = "staging"
	DirActive    = "active"   // reserved for future activation lifecycle phase
	DirPrevious  = "previous" // reserved for future rollback lifecycle phase

	// Standard file names within store directories.
	ModelFileName    = "model.json"
	MetadataFileName = "metadata.json"
)

// ModelStore domain errors.
var (
	ErrEmptyArtifact        = errors.New("empty model artifact data")
	ErrArtifactTooLarge     = errors.New("model artifact exceeds maximum allowed size")
	ErrWriteFailure         = errors.New("failed to write model artifact to storage")
	ErrReadFailure          = errors.New("failed to read model artifact from storage")
	ErrDeserializeFailure   = errors.New("failed to deserialize model artifact")
	ErrChecksumFailure      = errors.New("model artifact checksum verification failed")
	ErrValidationFailure    = errors.New("model artifact structural validation failed")
	ErrCompatibilityFailure = errors.New("model artifact is incompatible with edge runtime")
	ErrCandidateConflict    = errors.New("candidate model conflict: different checksum for existing model identity")
	ErrNoCandidateModel     = errors.New("no staged candidate model found")
	ErrInvalidStoreDir      = errors.New("invalid model store directory")
)

// StoreConfig configures the filesystem-backed model store.
type StoreConfig struct {
	BaseDir              string
	MaxArtifactSizeBytes int64
	CompatibilityReq     ml.CompatibilityRequirement
}

// StageRequest encapsulates the inputs for staging a candidate model artifact.
type StageRequest struct {
	Artifact         []byte
	Metadata         *ml.DeploymentMetadata
	CompatibilityReq *ml.CompatibilityRequirement
}

// StagedCandidate represents a validated, staged candidate model residing in the model store.
// The candidate model is NOT active and cannot evaluate live inference until an explicit activation occurs.
type StagedCandidate struct {
	Manifest       *ml.ModelManifest      `json:"manifest"`
	Metadata       *ml.DeploymentMetadata `json:"metadata"`
	ArtifactPath   string                 `json:"artifact_path"`
	MetadataPath   string                 `json:"metadata_path"`
	StagedAt       time.Time              `json:"staged_at"`
	ReusedExisting bool                   `json:"reused_existing"`
}

// ModelStore defines the local storage and staging boundary for edge ML model artifacts.
// Implementations MUST NOT activate candidates, modify inference behavior, or contact remote services.
type ModelStore interface {
	// StageModel receives, verifies, deserializes, validates, compatibility-checks,
	// and stages a model artifact into the candidate store.
	StageModel(ctx context.Context, req StageRequest) (*StagedCandidate, error)

	// GetCandidate retrieves the currently staged candidate model, if one exists.
	// Returns (nil, ErrNoCandidateModel) if no candidate is currently staged.
	GetCandidate(ctx context.Context) (*StagedCandidate, error)

	// ClearCandidate removes any currently staged candidate artifact and metadata.
	// Returns nil if no candidate was present.
	ClearCandidate(ctx context.Context) error

	// StoreDir returns the absolute base directory of the model store.
	StoreDir() string
}

// FileSystemModelStore implements ModelStore on the local filesystem.
// Provides safe Windows-compatible file operations, bounded artifact reading,
// non-destructive staging, idempotency, and conflict detection.
type FileSystemModelStore struct {
	baseDir          string
	stagingDir       string
	candidateDir     string
	maxSizeBytes     int64
	defaultCompatReq ml.CompatibilityRequirement
	mu               sync.Mutex
	tempCounter      uint64
}

// NewFileSystemModelStore initializes and validates a new FileSystemModelStore.
func NewFileSystemModelStore(cfg StoreConfig) (*FileSystemModelStore, error) {
	cleanBase := filepath.Clean(strings.TrimSpace(cfg.BaseDir))
	if cleanBase == "" || cleanBase == "." {
		return nil, fmt.Errorf("%w: base directory path cannot be empty", ErrInvalidStoreDir)
	}

	absBase, err := filepath.Abs(cleanBase)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to resolve absolute path %q: %v", ErrInvalidStoreDir, cleanBase, err)
	}

	stagingDir := filepath.Join(absBase, DirStaging)
	candidateDir := filepath.Join(absBase, DirCandidate)

	// Ensure directories exist
	if err := os.MkdirAll(stagingDir, 0700); err != nil {
		return nil, fmt.Errorf("%w: failed to create staging dir: %v", ErrWriteFailure, err)
	}
	if err := os.MkdirAll(candidateDir, 0700); err != nil {
		return nil, fmt.Errorf("%w: failed to create candidate dir: %v", ErrWriteFailure, err)
	}

	maxSize := cfg.MaxArtifactSizeBytes
	if maxSize <= 0 {
		maxSize = DefaultMaxArtifactSizeBytes
	}

	compatReq := cfg.CompatibilityReq
	if compatReq.SupportedAlgorithm == "" {
		compatReq = ml.DefaultCompatibilityRequirements()
	}

	store := &FileSystemModelStore{
		baseDir:          absBase,
		stagingDir:       stagingDir,
		candidateDir:     candidateDir,
		maxSizeBytes:     maxSize,
		defaultCompatReq: compatReq,
	}

	// Clean up any stale temporary staging files from prior interrupted operations
	store.cleanStagingDirectory()

	return store, nil
}

// StoreDir returns the absolute path of the model store directory.
func (s *FileSystemModelStore) StoreDir() string {
	return s.baseDir
}

// StageModel executes the multi-stage validation and staging flow for an incoming candidate artifact.
// The currently active inference model is never touched or modified.
func (s *FileSystemModelStore) StageModel(ctx context.Context, req StageRequest) (*StagedCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Input Validation: payload size bounds
	artifactLen := int64(len(req.Artifact))
	if artifactLen == 0 {
		return nil, ErrEmptyArtifact
	}
	if artifactLen > s.maxSizeBytes {
		return nil, fmt.Errorf("%w: artifact size %d bytes exceeds maximum limit %d bytes",
			ErrArtifactTooLarge, artifactLen, s.maxSizeBytes)
	}

	// Validate metadata if provided
	if req.Metadata != nil {
		if err := req.Metadata.Validate(); err != nil {
			return nil, fmt.Errorf("%w: invalid deployment metadata: %v", ErrValidationFailure, err)
		}
		if req.Metadata.ArtifactSizeBytes > 0 && req.Metadata.ArtifactSizeBytes != artifactLen {
			return nil, fmt.Errorf("%w: metadata artifact size %d does not match actual payload length %d",
				ErrValidationFailure, req.Metadata.ArtifactSizeBytes, artifactLen)
		}
	}

	compatReq := s.defaultCompatReq
	if req.CompatibilityReq != nil {
		compatReq = *req.CompatibilityReq
	}

	// 2. Generate unique staging temporary file path
	tmpPath := s.generateTempStagingPath()
	defer func() {
		// Clean up staging temporary file if it still exists
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
	}()

	// 3 & 4. Write artifact into isolated temporary staging file
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create temporary staging file: %v", ErrWriteFailure, err)
	}

	if _, err := tmpFile.Write(req.Artifact); err != nil {
		_ = tmpFile.Close()
		return nil, fmt.Errorf("%w: failed to write artifact to staging: %v", ErrWriteFailure, err)
	}

	// 5. Flush and close handle explicitly before any subsequent read operations (Windows compatibility)
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return nil, fmt.Errorf("%w: failed to sync staging file to disk: %v", ErrWriteFailure, err)
	}
	if err := tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("%w: failed to close staging file: %v", ErrWriteFailure, err)
	}

	// 6. Read artifact back with bounded reader from temporary staging path
	readBack, err := readBoundedFile(tmpPath, s.maxSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read back staging file: %v", ErrReadFailure, err)
	}

	// 7. Deserialize JSON into model manifest
	manifest, err := ml.Deserialize(readBack)
	if err != nil {
		if errors.Is(err, ml.ErrChecksumMismatch) {
			return nil, fmt.Errorf("%w: %v", ErrChecksumFailure, err)
		}
		if errors.Is(err, ml.ErrCorruptModelArtifact) && !strings.Contains(err.Error(), "JSON unmarshal error") {
			return nil, fmt.Errorf("%w: %v", ErrValidationFailure, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrDeserializeFailure, err)
	}

	// 8. Recompute and verify integrity checksum
	if strings.TrimSpace(manifest.ChecksumSHA256) == "" {
		return nil, fmt.Errorf("%w: manifest is missing required checksum_sha256", ErrChecksumFailure)
	}

	computedSum, err := manifest.ComputeChecksum()
	if err != nil {
		return nil, fmt.Errorf("%w: failed to compute manifest checksum: %v", ErrChecksumFailure, err)
	}

	if !strings.EqualFold(manifest.ChecksumSHA256, computedSum) {
		return nil, fmt.Errorf("%w: manifest checksum %q does not match computed checksum %q",
			ErrChecksumFailure, manifest.ChecksumSHA256, computedSum)
	}

	if req.Metadata != nil && strings.TrimSpace(req.Metadata.ChecksumSHA256) != "" {
		if !strings.EqualFold(req.Metadata.ChecksumSHA256, computedSum) {
			return nil, fmt.Errorf("%w: metadata checksum %q does not match computed checksum %q",
				ErrChecksumFailure, req.Metadata.ChecksumSHA256, computedSum)
		}
	}

	// 9. Structural & invariant validation
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrValidationFailure, err)
	}

	// 10. Runtime Compatibility Check
	compatReport := ml.CheckCompatibility(manifest, compatReq)
	if !compatReport.Compatible {
		return nil, fmt.Errorf("%w: %s", ErrCompatibilityFailure, strings.Join(compatReport.Issues, "; "))
	}

	// 11. Idempotency and Conflict Check against currently staged candidate
	candidateModelPath := filepath.Join(s.candidateDir, ModelFileName)
	candidateMetaPath := filepath.Join(s.candidateDir, MetadataFileName)

	if existingManifest, err := s.readCandidateManifest(); err == nil && existingManifest != nil {
		// Existing candidate is present: compare identity and checksum
		if existingManifest.ModelID == manifest.ModelID && existingManifest.ModelVersion == manifest.ModelVersion {
			if strings.EqualFold(existingManifest.ChecksumSHA256, manifest.ChecksumSHA256) {
				// Idempotent: exact same artifact is already staged
				existingMeta, _ := s.readCandidateMetadata()
				if existingMeta == nil {
					existingMeta = s.buildMetadataFromManifest(manifest, int64(len(readBack)), req.Metadata)
				}
				return &StagedCandidate{
					Manifest:       existingManifest.Clone(),
					Metadata:       existingMeta,
					ArtifactPath:   candidateModelPath,
					MetadataPath:   candidateMetaPath,
					StagedAt:       existingMeta.DeployedAt,
					ReusedExisting: true,
				}, nil
			}

			// Conflicting artifact: same model_id + model_version but different checksum
			return nil, fmt.Errorf("%w: candidate %s:%s already exists with checksum %s, refusing overwrite with checksum %s",
				ErrCandidateConflict, manifest.ModelID, manifest.ModelVersion,
				existingManifest.ChecksumSHA256, manifest.ChecksumSHA256)
		}
	}

	// 12. Promote artifact into candidate directory via temporary-file-based safe replacement.
	// Note: Crash-consistent activation is not implemented.
	stageCandidateTmp := filepath.Join(s.candidateDir, "model.json.tmp")
	if err := safeWriteReplace(stageCandidateTmp, candidateModelPath, readBack); err != nil {
		return nil, fmt.Errorf("%w: failed to promote model to candidate path: %v", ErrWriteFailure, err)
	}

	// Prepare and persist deployment metadata via temporary-file-based safe replacement
	metadata := s.buildMetadataFromManifest(manifest, int64(len(readBack)), req.Metadata)
	metaBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("%w: failed to serialize candidate metadata: %v", ErrWriteFailure, err)
	}

	stageMetaTmp := filepath.Join(s.candidateDir, "metadata.json.tmp")
	if err := safeWriteReplace(stageMetaTmp, candidateMetaPath, append(metaBytes, '\n')); err != nil {
		return nil, fmt.Errorf("%w: failed to persist candidate metadata: %v", ErrWriteFailure, err)
	}

	// 13. Return successfully staged candidate
	return &StagedCandidate{
		Manifest:       manifest.Clone(),
		Metadata:       metadata,
		ArtifactPath:   candidateModelPath,
		MetadataPath:   candidateMetaPath,
		StagedAt:       metadata.DeployedAt,
		ReusedExisting: false,
	}, nil
}

// GetCandidate returns the currently staged candidate model and metadata, or ErrNoCandidateModel if none exists.
func (s *FileSystemModelStore) GetCandidate(ctx context.Context) (*StagedCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidateModelPath := filepath.Join(s.candidateDir, ModelFileName)
	candidateMetaPath := filepath.Join(s.candidateDir, MetadataFileName)

	if _, err := os.Stat(candidateModelPath); os.IsNotExist(err) {
		return nil, ErrNoCandidateModel
	}

	data, err := readBoundedFile(candidateModelPath, s.maxSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read candidate model file: %v", ErrReadFailure, err)
	}

	manifest, err := ml.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("%w: corrupted candidate model file: %v", ErrValidationFailure, err)
	}

	meta, err := s.readCandidateMetadata()
	if err != nil || meta == nil {
		meta = s.buildMetadataFromManifest(manifest, int64(len(data)), nil)
	}

	return &StagedCandidate{
		Manifest:       manifest.Clone(),
		Metadata:       meta,
		ArtifactPath:   candidateModelPath,
		MetadataPath:   candidateMetaPath,
		StagedAt:       meta.DeployedAt,
		ReusedExisting: false,
	}, nil
}

// ClearCandidate removes the candidate model artifact and associated metadata from storage.
func (s *FileSystemModelStore) ClearCandidate(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidateModelPath := filepath.Join(s.candidateDir, ModelFileName)
	candidateMetaPath := filepath.Join(s.candidateDir, MetadataFileName)

	_ = os.Remove(candidateModelPath)
	_ = os.Remove(candidateMetaPath)

	return nil
}

// generateTempStagingPath creates a unique temporary filename within the staging directory.
func (s *FileSystemModelStore) generateTempStagingPath() string {
	ctr := atomic.AddUint64(&s.tempCounter, 1)
	nonce := make([]byte, 8)
	_, _ = rand.Read(nonce)
	filename := fmt.Sprintf("stage_%d_%d_%s.tmp", time.Now().UnixNano(), ctr, hex.EncodeToString(nonce))
	return filepath.Join(s.stagingDir, filename)
}

// readCandidateManifest reads and deserializes the existing candidate manifest, returning nil if absent.
func (s *FileSystemModelStore) readCandidateManifest() (*ml.ModelManifest, error) {
	candidateModelPath := filepath.Join(s.candidateDir, ModelFileName)
	if _, err := os.Stat(candidateModelPath); os.IsNotExist(err) {
		return nil, nil
	}

	data, err := readBoundedFile(candidateModelPath, s.maxSizeBytes)
	if err != nil {
		return nil, err
	}

	return ml.Deserialize(data)
}

// readCandidateMetadata reads the existing candidate metadata file, returning nil if absent.
func (s *FileSystemModelStore) readCandidateMetadata() (*ml.DeploymentMetadata, error) {
	candidateMetaPath := filepath.Join(s.candidateDir, MetadataFileName)
	if _, err := os.Stat(candidateMetaPath); os.IsNotExist(err) {
		return nil, nil
	}

	data, err := readBoundedFile(candidateMetaPath, 1024*1024)
	if err != nil {
		return nil, err
	}

	var meta ml.DeploymentMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

// buildMetadataFromManifest constructs canonical deployment metadata for a staged candidate.
func (s *FileSystemModelStore) buildMetadataFromManifest(
	manifest *ml.ModelManifest,
	artifactSize int64,
	userMeta *ml.DeploymentMetadata,
) *ml.DeploymentMetadata {
	deployedAt := time.Now().UTC()
	depID := fmt.Sprintf("dep-%s-%s-%d", manifest.ModelID, manifest.ModelVersion, deployedAt.UnixNano())
	targetFleet := ""
	targetNodeID := ""

	if userMeta != nil {
		if userMeta.DeploymentID != "" {
			depID = userMeta.DeploymentID
		}
		if !userMeta.DeployedAt.IsZero() {
			deployedAt = userMeta.DeployedAt
		}
		targetFleet = userMeta.TargetFleet
		targetNodeID = userMeta.TargetNodeID
	}

	return &ml.DeploymentMetadata{
		DeploymentID:          depID,
		ModelID:               manifest.ModelID,
		ModelVersion:          manifest.ModelVersion,
		ArtifactFormatVersion: manifest.ArtifactFormatVersion,
		FeatureSchemaVersion:  manifest.FeatureSchemaVersion,
		ChecksumSHA256:        manifest.ChecksumSHA256,
		ArtifactSizeBytes:     artifactSize,
		DeployedAt:            deployedAt,
		RollbackAuthorized:    false,
		TargetFleet:           targetFleet,
		TargetNodeID:          targetNodeID,
		SignatureInfo:         manifest.SignatureInfo,
	}
}

// cleanStagingDirectory removes any lingering temporary files from past interrupted operations.
func (s *FileSystemModelStore) cleanStagingDirectory() {
	entries, err := os.ReadDir(s.stagingDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".tmp") {
			_ = os.Remove(filepath.Join(s.stagingDir, entry.Name()))
		}
	}
}

// readBoundedFile opens and reads up to maxBytes from the target file, returning ErrArtifactTooLarge if exceeded.
func readBoundedFile(path string, maxBytes int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	limited := io.LimitReader(f, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}

	if int64(len(data)) > maxBytes {
		return nil, ErrArtifactTooLarge
	}

	return data, nil
}

// safeWriteReplace writes data to a temporary file, flushes/closes it, removes any existing target
// file (for Windows filesystem compatibility), and renames the temporary file into the final destination path.
// This provides temporary-file-based safe replacement against partially written files; crash-atomic
// transactional replacement across power loss or system crash is not claimed. Crash-consistent
// activation is not implemented.
func safeWriteReplace(tmpPath, finalPath string, data []byte) error {
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to open temp file %q: %w", tmpPath, err)
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to write data: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// On Windows, os.Rename fails if the destination already exists. Remove destination first if present.
	if _, err := os.Stat(finalPath); err == nil {
		_ = os.Remove(finalPath)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename %q to %q: %w", tmpPath, finalPath, err)
	}

	return nil
}
