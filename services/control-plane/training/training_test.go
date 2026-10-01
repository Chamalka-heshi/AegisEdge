package training

import (
	"bytes"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Chamalka-heshi/AegisEdge/shared/ml"
)

// helperGenerateDeterministicCSV returns a clean, straightforward CSV dataset.
func helperGenerateDeterministicCSV(n int) string {
	var sb strings.Builder
	sb.WriteString("cpu_usage_percent,memory_usage_percent,disk_usage_percent,temperature_celsius\n")
	for i := 0; i < n; i++ {
		fi := float64(i)
		cpu := 40.0 + 10.0*(fi/float64(n))
		mem := 50.0 + 10.0*(fi/float64(n))
		disk := 30.0 + 10.0*(fi/float64(n))
		temp := 45.0 + 10.0*(fi/float64(n))
		sb.WriteString(strings.Join([]string{
			strconv.FormatFloat(cpu, 'f', 4, 64),
			strconv.FormatFloat(mem, 'f', 4, 64),
			strconv.FormatFloat(disk, 'f', 4, 64),
			strconv.FormatFloat(temp, 'f', 4, 64),
		}, ",") + "\n")
	}
	return sb.String()
}

// -----------------------------------------------------------------------------
// A, B, C, D: DATASET PARSING, MALFORMED REJECTION, NAN/INF, FEATURE ORDERING
// -----------------------------------------------------------------------------

func TestDatasetParsing_Valid(t *testing.T) {
	csvData := `cpu_usage_percent,memory_usage_percent,disk_usage_percent,temperature_celsius
42.5,58.0,35.2,48.1
44.1,59.2,35.4,49.0
43.0,57.5,35.3,48.5`

	ds, err := LoadCSVDataset(strings.NewReader(csvData), "ds-01")
	if err != nil {
		t.Fatalf("expected successful parse, got: %v", err)
	}

	if len(ds.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(ds.Rows))
	}
	if ds.Rows[0][0] != 42.5 || ds.Rows[0][1] != 58.0 || ds.Rows[0][2] != 35.2 || ds.Rows[0][3] != 48.1 {
		t.Fatalf("row 0 values mismatch: %v", ds.Rows[0])
	}
}

func TestDatasetParsing_FeatureReordering(t *testing.T) {
	// Header in reversed order
	csvData := `temperature_celsius,disk_usage_percent,memory_usage_percent,cpu_usage_percent
48.1,35.2,58.0,42.5`

	ds, err := LoadCSVDataset(strings.NewReader(csvData), "ds-reordered")
	if err != nil {
		t.Fatalf("expected successful parse with reordering, got: %v", err)
	}

	// Must reorder to canonical: cpu (42.5), mem (58.0), disk (35.2), temp (48.1)
	if ds.Rows[0][0] != 42.5 || ds.Rows[0][1] != 58.0 || ds.Rows[0][2] != 35.2 || ds.Rows[0][3] != 48.1 {
		t.Fatalf("canonical reordering failed: %v", ds.Rows[0])
	}
}

func TestDatasetParsing_MalformedHeaderAndMissingColumns(t *testing.T) {
	// Missing header
	_, err := LoadCSVDataset(strings.NewReader(""), "ds-empty")
	if err == nil || !strings.Contains(err.Error(), "missing header") {
		t.Fatalf("expected missing header error, got: %v", err)
	}

	// Missing required column (temperature_celsius replaced with fan_speed)
	missingCol := `cpu_usage_percent,memory_usage_percent,disk_usage_percent,fan_speed
42.5,58.0,35.2,1200`
	_, err = LoadCSVDataset(strings.NewReader(missingCol), "ds-missing")
	if err == nil || !strings.Contains(err.Error(), "not a recognized telemetry metric") {
		t.Fatalf("expected unrecognized column error, got: %v", err)
	}

	// Duplicate column
	dupCol := `cpu_usage_percent,memory_usage_percent,disk_usage_percent,cpu_usage_percent
42.5,58.0,35.2,42.5`
	_, err = LoadCSVDataset(strings.NewReader(dupCol), "ds-dup")
	if err == nil || !strings.Contains(err.Error(), "duplicate column") {
		t.Fatalf("expected duplicate column error, got: %v", err)
	}
}

func TestDatasetParsing_NaNInfAndNonNumeric(t *testing.T) {
	// Non-numeric
	nonNumeric := `cpu_usage_percent,memory_usage_percent,disk_usage_percent,temperature_celsius
42.5,ERROR,35.2,48.1`
	_, err := LoadCSVDataset(strings.NewReader(nonNumeric), "ds-nan")
	if err == nil || !strings.Contains(err.Error(), "non-numeric") {
		t.Fatalf("expected non-numeric error, got: %v", err)
	}

	// NaN
	nanCSV := `cpu_usage_percent,memory_usage_percent,disk_usage_percent,temperature_celsius
42.5,NaN,35.2,48.1`
	_, err = LoadCSVDataset(strings.NewReader(nanCSV), "ds-nan")
	if err == nil || !strings.Contains(err.Error(), "non-finite") {
		t.Fatalf("expected non-finite error for NaN, got: %v", err)
	}

	// +Inf
	infCSV := `cpu_usage_percent,memory_usage_percent,disk_usage_percent,temperature_celsius
42.5,+Inf,35.2,48.1`
	_, err = LoadCSVDataset(strings.NewReader(infCSV), "ds-inf")
	if err == nil || !strings.Contains(err.Error(), "non-finite") {
		t.Fatalf("expected non-finite error for +Inf, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// E, F, G, H, I, J: TRAINING, DETERMINISM, SEED SENSITIVITY, TREE STRUCTURE
// -----------------------------------------------------------------------------

func TestTraining_DeterministicOutput(t *testing.T) {
	csvStr := helperGenerateDeterministicCSV(300)
	ds, err := LoadCSVDataset(strings.NewReader(csvStr), "ds-test")
	if err != nil {
		t.Fatalf("dataset load failed: %v", err)
	}

	cfg := DefaultTrainConfig()
	cfg.Seed = 12345

	m1, err := Train(ds, cfg)
	if err != nil {
		t.Fatalf("training 1 failed: %v", err)
	}

	m2, err := Train(ds, cfg)
	if err != nil {
		t.Fatalf("training 2 failed: %v", err)
	}

	if m1.ChecksumSHA256 != m2.ChecksumSHA256 {
		t.Fatalf("checksums must be identical for identical seed: %s != %s",
			m1.ChecksumSHA256, m2.ChecksumSHA256)
	}

	if len(m1.Trees) != len(m2.Trees) {
		t.Fatalf("tree count mismatch")
	}

	// Check tree node splits match exactly
	for i := range m1.Trees {
		t1 := m1.Trees[i]
		t2 := m2.Trees[i]
		if len(t1.Nodes) != len(t2.Nodes) {
			t.Fatalf("tree %d node count mismatch", i)
		}
		for n := range t1.Nodes {
			if t1.Nodes[n] != t2.Nodes[n] {
				t.Fatalf("tree %d node %d mismatch: %+v != %+v", i, n, t1.Nodes[n], t2.Nodes[n])
			}
		}
	}
}

func TestTraining_SeedSensitivity(t *testing.T) {
	csvStr := helperGenerateDeterministicCSV(300)
	ds, err := LoadCSVDataset(strings.NewReader(csvStr), "ds-test")
	if err != nil {
		t.Fatalf("dataset load failed: %v", err)
	}

	cfg1 := DefaultTrainConfig()
	cfg1.Seed = 42

	cfg2 := DefaultTrainConfig()
	cfg2.Seed = 99999

	m1, err := Train(ds, cfg1)
	if err != nil {
		t.Fatalf("training 1 failed: %v", err)
	}

	m2, err := Train(ds, cfg2)
	if err != nil {
		t.Fatalf("training 2 failed: %v", err)
	}

	// Different seeds must produce different tree partitions and different checksums
	if m1.ChecksumSHA256 == m2.ChecksumSHA256 {
		t.Fatalf("different seeds produced identical checksums! seed sensitivity failed")
	}
}

func TestTraining_ConfigValidationLimits(t *testing.T) {
	ds := &TrainingDataset{
		DatasetID:    "ds",
		FeatureNames: ml.CanonicalSupportedMetrics,
		Rows:         [][]float64{{40, 50, 30, 45}, {45, 55, 35, 50}},
	}

	// Trees exceed MaxTrees
	cfg1 := DefaultTrainConfig()
	cfg1.Trees = ml.MaxTrees + 1
	if _, err := Train(ds, cfg1); err == nil || !strings.Contains(err.Error(), "trees must be in") {
		t.Fatalf("expected excessive trees error, got: %v", err)
	}

	// Subsample < 2
	cfg2 := DefaultTrainConfig()
	cfg2.SubSampleSize = 1
	if _, err := Train(ds, cfg2); err == nil || !strings.Contains(err.Error(), "sub_sample_size") {
		t.Fatalf("expected invalid subsample error, got: %v", err)
	}

	// Threshold out of range
	cfg3 := DefaultTrainConfig()
	cfg3.DecisionThreshold = 1.5
	if _, err := Train(ds, cfg3); err == nil || !strings.Contains(err.Error(), "decision_threshold") {
		t.Fatalf("expected invalid threshold error, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// K, L, M, N, O, P, Q, R: SERIALIZATION, DESERIALIZATION, ARTIFACT DETERMINISM,
// AND END-TO-END INFERENCE EQUIVALENCE
// -----------------------------------------------------------------------------

func TestSerialization_ByteForByteDeterminism(t *testing.T) {
	csvStr := helperGenerateDeterministicCSV(300)
	ds, _ := LoadCSVDataset(strings.NewReader(csvStr), "ds-det")

	cfg := DefaultTrainConfig()
	cfg.Seed = 777

	m1, _ := Train(ds, cfg)
	m2, _ := Train(ds, cfg)

	b1, err := SerializeModel(m1)
	if err != nil {
		t.Fatalf("serialization 1 failed: %v", err)
	}

	b2, err := SerializeModel(m2)
	if err != nil {
		t.Fatalf("serialization 2 failed: %v", err)
	}

	if !bytes.Equal(b1, b2) {
		t.Fatalf("serialized model artifacts are not byte-for-byte identical!\nlen1=%d, len2=%d", len(b1), len(b2))
	}
}

func TestSerialization_CorruptArtifactRejection(t *testing.T) {
	// Empty data
	_, err := DeserializeModel([]byte(""))
	if err == nil || !strings.Contains(err.Error(), "empty data") {
		t.Fatalf("expected empty data error, got: %v", err)
	}

	// Malformed JSON
	_, err = DeserializeModel([]byte("{ invalid json }"))
	if err == nil || !strings.Contains(err.Error(), "JSON unmarshal error") {
		t.Fatalf("expected unmarshal error, got: %v", err)
	}

	// Tampered checksum
	csvStr := helperGenerateDeterministicCSV(100)
	ds, _ := LoadCSVDataset(strings.NewReader(csvStr), "ds-tamper")
	m, _ := Train(ds, DefaultTrainConfig())
	data, _ := SerializeModel(m)

	tamperedChecksum := strings.Replace(string(data), m.ChecksumSHA256, "0000000000000000000000000000000000000000000000000000000000000000", 1)
	_, err = DeserializeModel([]byte(tamperedChecksum))
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error on tampered checksum, got: %v", err)
	}

	// Tampered payload while keeping original checksum
	tamperedPayload := strings.Replace(string(data), `"model_version": "1.0.0"`, `"model_version": "2.0.0"`, 1)
	if tamperedPayload == string(data) {
		t.Fatalf("test setup error: failed to replace model_version in serialized payload")
	}
	_, err = DeserializeModel([]byte(tamperedPayload))
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error on tampered payload with original checksum, got: %v", err)
	}
}

func TestEndToEnd_TrainSerializeLoadEquivalence(t *testing.T) {
	tmpDir := t.TempDir()
	modelPath := filepath.Join(tmpDir, "trained_model.json")

	csvStr := helperGenerateDeterministicCSV(300)
	ds, err := LoadCSVDataset(strings.NewReader(csvStr), "ds-e2e")
	if err != nil {
		t.Fatalf("dataset load failed: %v", err)
	}

	cfg := DefaultTrainConfig()
	cfg.Seed = 42
	cfg.Trees = 50
	cfg.SubSampleSize = 128
	cfg.DecisionThreshold = 0.60

	// 1. Train in-memory model
	inMemoryManifest, err := Train(ds, cfg)
	if err != nil {
		t.Fatalf("training failed: %v", err)
	}

	// 2. Save model to file (includes post-write verification)
	if err := SaveModelToFile(inMemoryManifest, modelPath); err != nil {
		t.Fatalf("save to file failed: %v", err)
	}

	// 3. Load model from file
	loadedManifest, err := LoadModelFromFile(modelPath)
	if err != nil {
		t.Fatalf("load from file failed: %v", err)
	}

	// 4. Assert model properties match exactly
	if loadedManifest.ChecksumSHA256 != inMemoryManifest.ChecksumSHA256 {
		t.Fatalf("checksum mismatch: %s != %s", loadedManifest.ChecksumSHA256, inMemoryManifest.ChecksumSHA256)
	}
	if err := loadedManifest.Validate(); err != nil {
		t.Fatalf("loaded manifest validation failed: %v", err)
	}
	if loadedManifest.ModelID != inMemoryManifest.ModelID {
		t.Fatalf("model ID mismatch: %s != %s", loadedManifest.ModelID, inMemoryManifest.ModelID)
	}
	if loadedManifest.ModelVersion != inMemoryManifest.ModelVersion {
		t.Fatalf("model version mismatch: %s != %s", loadedManifest.ModelVersion, inMemoryManifest.ModelVersion)
	}
	if len(loadedManifest.Trees) != len(inMemoryManifest.Trees) {
		t.Fatalf("tree count mismatch: %d != %d", len(loadedManifest.Trees), len(inMemoryManifest.Trees))
	}
	if loadedManifest.DecisionThreshold != inMemoryManifest.DecisionThreshold {
		t.Fatalf("threshold mismatch: %f != %f", loadedManifest.DecisionThreshold, inMemoryManifest.DecisionThreshold)
	}
	if loadedManifest.SubSampleSize != inMemoryManifest.SubSampleSize {
		t.Fatalf("subsample size mismatch: %d != %d", loadedManifest.SubSampleSize, inMemoryManifest.SubSampleSize)
	}
	if len(loadedManifest.NormalizationParams) != len(inMemoryManifest.NormalizationParams) {
		t.Fatalf("normalization params count mismatch")
	}

	// 5. Assert canonical serialized byte-for-byte equivalence
	rawInMemory, err := SerializeModel(inMemoryManifest)
	if err != nil {
		t.Fatalf("serializing inMemoryManifest failed: %v", err)
	}
	rawLoaded, err := SerializeModel(loadedManifest)
	if err != nil {
		t.Fatalf("serializing loadedManifest failed: %v", err)
	}
	if !bytes.Equal(rawInMemory, rawLoaded) {
		t.Fatalf("serialized representations do not match byte-for-byte")
	}
}

// -----------------------------------------------------------------------------
// EDGE CASES: DATASET SIZES, ZERO VARIANCE, REPEATED ROWS
// -----------------------------------------------------------------------------

func TestEdgeCases_ZeroVarianceFeatureRejected(t *testing.T) {
	// CPU usage is constant at 50.0 across all rows -> stddev = 0
	csvData := `cpu_usage_percent,memory_usage_percent,disk_usage_percent,temperature_celsius
50.0,55.0,35.0,45.0
50.0,60.0,36.0,46.0
50.0,58.0,34.0,47.0`

	ds, err := LoadCSVDataset(strings.NewReader(csvData), "ds-zero-var")
	if err != nil {
		t.Fatalf("dataset load failed: %v", err)
	}

	_, err = Train(ds, DefaultTrainConfig())
	if err == nil || !strings.Contains(err.Error(), "zero variance") {
		t.Fatalf("expected zero variance rejection, got: %v", err)
	}
}

func TestEdgeCases_FewerSamplesThanSubSampleSize(t *testing.T) {
	// Dataset has only 5 samples, but SubSampleSize is 256
	// Must succeed: subsampling uses all 5 samples without panic or error
	csvData := `cpu_usage_percent,memory_usage_percent,disk_usage_percent,temperature_celsius
40.0,50.0,30.0,45.0
42.0,52.0,32.0,46.0
44.0,54.0,34.0,47.0
46.0,56.0,36.0,48.0
48.0,58.0,38.0,49.0`

	ds, err := LoadCSVDataset(strings.NewReader(csvData), "ds-small")
	if err != nil {
		t.Fatalf("dataset load failed: %v", err)
	}

	cfg := DefaultTrainConfig()
	cfg.Trees = 10
	cfg.SubSampleSize = 256

	manifest, err := Train(ds, cfg)
	if err != nil {
		t.Fatalf("training on small dataset failed: %v", err)
	}

	if len(manifest.Trees) != 10 {
		t.Fatalf("expected 10 trees, got %d", len(manifest.Trees))
	}
}

func TestEdgeCases_ExactlySubSampleSize(t *testing.T) {
	csvStr := helperGenerateDeterministicCSV(256)
	ds, _ := LoadCSVDataset(strings.NewReader(csvStr), "ds-256")

	cfg := DefaultTrainConfig()
	cfg.Trees = 10
	cfg.SubSampleSize = 256

	manifest, err := Train(ds, cfg)
	if err != nil {
		t.Fatalf("training exactly psi samples failed: %v", err)
	}

	if len(manifest.Trees) != 10 {
		t.Fatalf("expected 10 trees, got %d", len(manifest.Trees))
	}
}

// -----------------------------------------------------------------------------
// BENCHMARK (OBSERVED MEASUREMENT ONLY)
// -----------------------------------------------------------------------------

func BenchmarkTraining_100Trees(b *testing.B) {
	csvStr := helperGenerateDeterministicCSV(500)
	ds, err := LoadCSVDataset(strings.NewReader(csvStr), "ds-bench")
	if err != nil {
		b.Fatalf("dataset load failed: %v", err)
	}

	cfg := DefaultTrainConfig()
	cfg.Trees = 100
	cfg.SubSampleSize = 256

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Train(ds, cfg)
	}
}
