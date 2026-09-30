package training

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/detector"
)

// Dataset parsing errors.
var (
	ErrEmptyDataset          = errors.New("training dataset is empty")
	ErrMissingHeader         = errors.New("training dataset CSV is missing header row")
	ErrMissingRequiredColumn = errors.New("training dataset CSV missing required canonical column")
	ErrUnsupportedColumn     = errors.New("unsupported column in training dataset CSV header")
	ErrDuplicateColumn       = errors.New("duplicate column in training dataset CSV header")
	ErrInvalidRowDimension   = errors.New("row column count does not match header dimension")
	ErrNonNumericValue       = errors.New("non-numeric feature value encountered in dataset")
	ErrNonFiniteValue        = errors.New("non-finite feature value (NaN or Inf) encountered in dataset")
)

// TrainingDataset contains validated, parsed telemetry samples arranged in canonical 4D feature ordering.
type TrainingDataset struct {
	DatasetID    string
	FeatureNames []string
	Rows         [][]float64 // each row is guaranteed len=4: [cpu_usage_percent, memory_usage_percent, disk_usage_percent, temperature_celsius]
}

// LoadCSVDataset reads and validates a CSV dataset from an io.Reader.
//
// Rules:
//  1. The header must contain all four canonical metrics defined by detector.CanonicalSupportedMetrics:
//     "cpu_usage_percent", "memory_usage_percent", "disk_usage_percent", "temperature_celsius".
//  2. No unsupported columns or duplicate columns are permitted.
//  3. Every data row must have exactly 4 columns.
//  4. Every cell must be a valid, finite float64 (no NaN, +Inf, -Inf, or empty strings).
//  5. The dataset must contain at least one valid data row.
//  6. Extracted rows are reordered into canonical schema order regardless of column order in the CSV header.
func LoadCSVDataset(r io.Reader, datasetID string) (*TrainingDataset, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrMissingHeader
		}
		return nil, fmt.Errorf("%w: %w", ErrMissingHeader, err)
	}

	if len(header) != len(detector.CanonicalSupportedMetrics) {
		return nil, fmt.Errorf("%w: expected %d columns, got %d",
			ErrInvalidRowDimension, len(detector.CanonicalSupportedMetrics), len(header))
	}

	// Map header columns to canonical feature indices
	colToCanonical := make([]int, len(header))
	seenColumns := make(map[string]struct{}, len(header))

	for colIdx, rawName := range header {
		name := strings.TrimSpace(rawName)
		if name == "" {
			return nil, fmt.Errorf("%w: empty column header at index %d", ErrUnsupportedColumn, colIdx)
		}
		if _, seen := seenColumns[name]; seen {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateColumn, name)
		}
		seenColumns[name] = struct{}{}

		canonicalIdx := -1
		for i, canon := range detector.CanonicalSupportedMetrics {
			if name == canon {
				canonicalIdx = i
				break
			}
		}
		if canonicalIdx == -1 {
			return nil, fmt.Errorf("%w: %q is not a recognized telemetry metric", ErrUnsupportedColumn, name)
		}
		colToCanonical[colIdx] = canonicalIdx
	}

	// Ensure all canonical metrics were present
	for _, canon := range detector.CanonicalSupportedMetrics {
		if _, ok := seenColumns[canon]; !ok {
			return nil, fmt.Errorf("%w: %q", ErrMissingRequiredColumn, canon)
		}
	}

	var rows [][]float64
	lineNum := 1

	for {
		lineNum++
		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read line %d: %w", lineNum, err)
		}

		if len(record) != len(header) {
			return nil, fmt.Errorf("%w at line %d: expected %d columns, got %d",
				ErrInvalidRowDimension, lineNum, len(header), len(record))
		}

		canonicalRow := make([]float64, len(detector.CanonicalSupportedMetrics))
		for colIdx, valStr := range record {
			trimmed := strings.TrimSpace(valStr)
			val, pErr := strconv.ParseFloat(trimmed, 64)
			if pErr != nil {
				return nil, fmt.Errorf("%w at line %d, col %d (%q): %w",
					ErrNonNumericValue, lineNum, colIdx, trimmed, pErr)
			}
			if math.IsNaN(val) || math.IsInf(val, 0) {
				return nil, fmt.Errorf("%w at line %d, col %d (%f)",
					ErrNonFiniteValue, lineNum, colIdx, val)
			}
			canonicalIdx := colToCanonical[colIdx]
			canonicalRow[canonicalIdx] = val
		}

		rows = append(rows, canonicalRow)
	}

	if len(rows) == 0 {
		return nil, ErrEmptyDataset
	}

	id := strings.TrimSpace(datasetID)
	if id == "" {
		id = "dataset-default"
	}

	return &TrainingDataset{
		DatasetID:    id,
		FeatureNames: detector.CanonicalSupportedMetrics,
		Rows:         rows,
	}, nil
}

// LoadCSVDatasetFile reads and validates a CSV dataset directly from a filesystem path.
func LoadCSVDatasetFile(filePath, datasetID string) (*TrainingDataset, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open dataset file %q: %w", filePath, err)
	}
	defer f.Close()

	return LoadCSVDataset(f, datasetID)
}
