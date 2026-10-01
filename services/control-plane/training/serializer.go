package training

import (
	"errors"
	"fmt"
	"os"

	"github.com/Chamalka-heshi/AegisEdge/shared/ml"
)

// Serialization errors.
var (
	ErrNilManifestToSerialize = errors.New("cannot serialize nil model manifest")
	ErrCorruptModelArtifact   = errors.New("corrupt model artifact")
)

// SerializeModel serializes an MLModelManifest into deterministic, formatted JSON bytes.
// Validates the manifest before serializing.
func SerializeModel(manifest *ml.ModelManifest) ([]byte, error) {
	if manifest == nil {
		return nil, ErrNilManifestToSerialize
	}
	return ml.Serialize(manifest)
}

// DeserializeModel unmarshals JSON bytes into an MLModelManifest and strictly validates it.
func DeserializeModel(data []byte) (*ml.ModelManifest, error) {
	return ml.Deserialize(data)
}

// SaveModelToFile writes an MLModelManifest to a local file.
// Validates before writing, and performs an immediate read-back verification to guarantee round-trip integrity.
func SaveModelToFile(manifest *ml.ModelManifest, filePath string) error {
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
func LoadModelFromFile(filePath string) (*ml.ModelManifest, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read model file %q: %w", filePath, err)
	}

	return DeserializeModel(data)
}
