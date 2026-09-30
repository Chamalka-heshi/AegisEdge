package training

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/detector"
)

// Serialization errors.
var (
	ErrNilManifestToSerialize = errors.New("cannot serialize nil model manifest")
	ErrCorruptModelArtifact   = errors.New("corrupt model artifact")
)

// SerializeModel serializes an MLModelManifest into deterministic, formatted JSON bytes.
// Validates the manifest before serializing.
func SerializeModel(manifest *detector.MLModelManifest) ([]byte, error) {
	if manifest == nil {
		return nil, ErrNilManifestToSerialize
	}

	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("refusing to serialize invalid model manifest: %w", err)
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal model manifest: %w", err)
	}

	// Append trailing newline for POSIX/editor compatibility
	return append(data, '\n'), nil
}

// DeserializeModel unmarshals JSON bytes into an MLModelManifest and strictly validates it.
func DeserializeModel(data []byte) (*detector.MLModelManifest, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty data", ErrCorruptModelArtifact)
	}

	var manifest detector.MLModelManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("%w: JSON unmarshal error: %w", ErrCorruptModelArtifact, err)
	}

	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("%w: edge validation failed: %w", ErrCorruptModelArtifact, err)
	}

	return &manifest, nil
}

// SaveModelToFile writes an MLModelManifest to a local file.
// Validates before writing, and performs an immediate read-back verification to guarantee round-trip integrity.
func SaveModelToFile(manifest *detector.MLModelManifest, filePath string) error {
	data, err := SerializeModel(manifest)
	if err != nil {
		return err
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write model file %q: %w", filePath, err)
	}

	// Verify the written file can be read and validated
	readBack, err := LoadModelFromFile(filePath)
	if err != nil {
		return fmt.Errorf("model post-write verification failed: %w", err)
	}

	if readBack.ChecksumSHA256 != manifest.ChecksumSHA256 {
		return fmt.Errorf("model checksum post-write mismatch: %s != %s",
			readBack.ChecksumSHA256, manifest.ChecksumSHA256)
	}

	return nil
}

// LoadModelFromFile reads, unmarshals, and validates an MLModelManifest from a file.
func LoadModelFromFile(filePath string) (*detector.MLModelManifest, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read model file %q: %w", filePath, err)
	}

	return DeserializeModel(data)
}
