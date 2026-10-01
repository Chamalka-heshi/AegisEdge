package training

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/ml"
)

// Training configuration and execution errors.
var (
	ErrInvalidTrainingConfig = errors.New("invalid training configuration")
	ErrZeroVarianceFeature   = errors.New("feature exhibits zero variance across training dataset")
	ErrInsufficientSamples   = errors.New("training dataset has insufficient samples")
)

// TrainConfig configures the offline Isolation Forest training run.
type TrainConfig struct {
	ModelID           string
	ModelVersion      string
	TrainingDatasetID string
	Trees             int
	SubSampleSize     int
	Seed              int64
	MaxTreeDepth      int
	DecisionThreshold float64
	CreatedAt         time.Time // if zero, defaults to deterministic epoch for reproducibility
}

// DefaultTrainConfig returns standard, safe baseline training hyperparameters.
func DefaultTrainConfig() TrainConfig {
	return TrainConfig{
		ModelID:           "iforest-v1",
		ModelVersion:      "1.0.0",
		TrainingDatasetID: "ds-baseline",
		Trees:             100,
		SubSampleSize:     256,
		Seed:              42,
		MaxTreeDepth:      0, // 0 derives ceil(log2(SubSampleSize))
		DecisionThreshold: 0.60,
		CreatedAt:         time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

// Validate verifies that the training configuration conforms to domain requirements and safety bounds.
func (cfg *TrainConfig) Validate() error {
	if strings.TrimSpace(cfg.ModelID) == "" {
		return fmt.Errorf("%w: model_id cannot be empty", ErrInvalidTrainingConfig)
	}
	if strings.TrimSpace(cfg.ModelVersion) == "" {
		return fmt.Errorf("%w: model_version cannot be empty", ErrInvalidTrainingConfig)
	}
	if cfg.Trees < 1 || cfg.Trees > ml.MaxTrees {
		return fmt.Errorf("%w: trees must be in [1, %d], got %d",
			ErrInvalidTrainingConfig, ml.MaxTrees, cfg.Trees)
	}
	if cfg.SubSampleSize < 2 || cfg.SubSampleSize > ml.MaxSubSampleSize {
		return fmt.Errorf("%w: sub_sample_size must be in [2, %d], got %d",
			ErrInvalidTrainingConfig, ml.MaxSubSampleSize, cfg.SubSampleSize)
	}
	if math.IsNaN(cfg.DecisionThreshold) || math.IsInf(cfg.DecisionThreshold, 0) ||
		cfg.DecisionThreshold <= 0.0 || cfg.DecisionThreshold >= 1.0 {
		return fmt.Errorf("%w: decision_threshold must be in (0.0, 1.0), got %f",
			ErrInvalidTrainingConfig, cfg.DecisionThreshold)
	}
	if cfg.MaxTreeDepth > ml.MaxTreeDepth {
		return fmt.Errorf("%w: max_tree_depth cannot exceed %d, got %d",
			ErrInvalidTrainingConfig, ml.MaxTreeDepth, cfg.MaxTreeDepth)
	}
	return nil
}

// ComputeNormalizationParams computes the mean, standard deviation, min, and max for each feature
// strictly over the provided training dataset.
// Rejects datasets where any feature has zero or near-zero variance (stddev <= 1e-9).
func ComputeNormalizationParams(dataset *TrainingDataset) ([]ml.FeatureNormalizationParams, error) {
	if dataset == nil || len(dataset.Rows) == 0 {
		return nil, ErrEmptyDataset
	}

	numRows := len(dataset.Rows)
	numCols := len(ml.CanonicalSupportedMetrics)
	params := make([]ml.FeatureNormalizationParams, numCols)

	for col := 0; col < numCols; col++ {
		metricName := ml.CanonicalSupportedMetrics[col]

		var sum float64
		minVal := dataset.Rows[0][col]
		maxVal := dataset.Rows[0][col]

		for row := 0; row < numRows; row++ {
			v := dataset.Rows[row][col]
			sum += v
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}

		mean := sum / float64(numRows)

		var varSum float64
		for row := 0; row < numRows; row++ {
			diff := dataset.Rows[row][col] - mean
			varSum += diff * diff
		}

		stdDev := math.Sqrt(varSum / float64(numRows))

		if stdDev <= 1e-9 {
			return nil, fmt.Errorf("%w: metric %q has stddev %f (min=%f, max=%f)",
				ErrZeroVarianceFeature, metricName, stdDev, minVal, maxVal)
		}

		params[col] = ml.FeatureNormalizationParams{
			MetricName: metricName,
			Mean:       mean,
			StdDev:     stdDev,
			Min:        minVal,
			Max:        maxVal,
		}
	}

	return params, nil
}

// NormalizeDataset applies frozen normalization parameters to the training dataset rows.
func NormalizeDataset(dataset *TrainingDataset, params []ml.FeatureNormalizationParams) [][]float64 {
	numRows := len(dataset.Rows)
	numCols := len(params)
	normalized := make([][]float64, numRows)

	for r := 0; r < numRows; r++ {
		normRow := make([]float64, numCols)
		for c := 0; c < numCols; c++ {
			normRow[c] = (dataset.Rows[r][c] - params[c].Mean) / params[c].StdDev
		}
		normalized[r] = normRow
	}

	return normalized
}

// Train executes the offline Isolation Forest training algorithm over a validated TrainingDataset.
// Produces an immutable, fully validated MLModelManifest ready for deployment to edge nodes.
func Train(dataset *TrainingDataset, cfg TrainConfig) (*ml.ModelManifest, error) {
	if dataset == nil || len(dataset.Rows) == 0 {
		return nil, ErrEmptyDataset
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// 1. Calculate frozen normalization parameters over training baseline
	normParams, err := ComputeNormalizationParams(dataset)
	if err != nil {
		return nil, fmt.Errorf("normalization parameter computation failed: %w", err)
	}

	// 2. Pre-normalize training rows
	normRows := NormalizeDataset(dataset, normParams)
	totalRows := len(normRows)

	// 3. Initialize deterministic pseudo-random number generator
	prng := rand.New(rand.NewSource(cfg.Seed))

	// 4. Derive maximum tree height: ceil(log2(SubSampleSize))
	maxDepth := cfg.MaxTreeDepth
	if maxDepth <= 0 {
		maxDepth = int(math.Ceil(math.Log2(float64(cfg.SubSampleSize))))
	}
	if maxDepth > ml.MaxTreeDepth {
		maxDepth = ml.MaxTreeDepth
	}

	// 5. Construct isolation trees
	trees := make([]ml.IsolationTree, cfg.Trees)

	for t := 0; t < cfg.Trees; t++ {
		// Subsample selection without replacement
		subsample := selectSubsample(normRows, cfg.SubSampleSize, totalRows, prng)

		var nodes []ml.IsolationTreeNode
		buildIsolationTree(subsample, 0, maxDepth, prng, &nodes)

		trees[t] = ml.IsolationTree{
			RootIndex: 0,
			Nodes:     nodes,
		}
	}

	createdAt := cfg.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	datasetID := cfg.TrainingDatasetID
	if datasetID == "" {
		datasetID = dataset.DatasetID
	}

	manifest := &ml.ModelManifest{
		ModelID:              cfg.ModelID,
		ModelVersion:         cfg.ModelVersion,
		Algorithm:            ml.AlgorithmIsolationForest,
		FeatureSchemaVersion: ml.FeatureSchemaV1,
		TrainingDatasetID:    datasetID,
		CreatedAt:            createdAt,
		InputDimensions:      len(ml.CanonicalSupportedMetrics),
		SupportedMetrics:     ml.CanonicalSupportedMetrics,
		NormalizationParams:  normParams,
		Trees:                trees,
		SubSampleSize:        cfg.SubSampleSize,
		DecisionThreshold:    cfg.DecisionThreshold,
		Status:               ml.ModelStatusActive,
	}

	// 6. Compute integrity checksum
	checksum, err := manifest.ComputeChecksum()
	if err != nil {
		return nil, fmt.Errorf("failed to compute model checksum: %w", err)
	}
	manifest.ChecksumSHA256 = checksum

	// 7. Verify model satisfies all edge validation invariants
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("trained model manifest failed edge validation: %w", err)
	}

	return manifest, nil
}

// selectSubsample draws a random subsample of size psi from rows without replacement using the PRNG.
func selectSubsample(rows [][]float64, psi, totalRows int, prng *rand.Rand) [][]float64 {
	if totalRows <= psi {
		sample := make([][]float64, totalRows)
		copy(sample, rows)
		return sample
	}

	// Partial Fisher-Yates shuffle
	indices := make([]int, totalRows)
	for i := 0; i < totalRows; i++ {
		indices[i] = i
	}

	sample := make([][]float64, psi)
	for i := 0; i < psi; i++ {
		j := i + prng.Intn(totalRows-i)
		indices[i], indices[j] = indices[j], indices[i]
		sample[i] = rows[indices[i]]
	}

	return sample
}

// buildIsolationTree recursively partitions data until a leaf termination condition is met.
func buildIsolationTree(data [][]float64, depth, maxDepth int, prng *rand.Rand, nodes *[]ml.IsolationTreeNode) int {
	myIdx := len(*nodes)
	// Reserve slot in nodes slice
	*nodes = append(*nodes, ml.IsolationTreeNode{})

	n := len(data)
	if depth >= maxDepth || n <= 1 {
		(*nodes)[myIdx] = ml.IsolationTreeNode{
			FeatureIndex: -1,
			SplitValue:   0.0,
			LeftChild:    -1,
			RightChild:   -1,
			Size:         n,
		}
		return myIdx
	}

	// Check if all instances are identical across all features
	numCols := len(data[0])
	var eligibleFeatures []int
	minVals := make([]float64, numCols)
	maxVals := make([]float64, numCols)

	for c := 0; c < numCols; c++ {
		minV := data[0][c]
		maxV := data[0][c]
		for r := 1; r < n; r++ {
			v := data[r][c]
			if v < minV {
				minV = v
			}
			if v > maxV {
				maxV = v
			}
		}
		minVals[c] = minV
		maxVals[c] = maxV
		if maxV > minV {
			eligibleFeatures = append(eligibleFeatures, c)
		}
	}

	if len(eligibleFeatures) == 0 {
		// All data points identical: cannot split further
		(*nodes)[myIdx] = ml.IsolationTreeNode{
			FeatureIndex: -1,
			SplitValue:   0.0,
			LeftChild:    -1,
			RightChild:   -1,
			Size:         n,
		}
		return myIdx
	}

	// Randomly select one eligible feature
	q := eligibleFeatures[prng.Intn(len(eligibleFeatures))]
	minQ := minVals[q]
	maxQ := maxVals[q]

	// Uniformly pick split value p in (minQ, maxQ)
	p := minQ + prng.Float64()*(maxQ-minQ)

	// Partition data into left (< p) and right (>= p)
	var left [][]float64
	var right [][]float64

	for r := 0; r < n; r++ {
		if data[r][q] < p {
			left = append(left, data[r])
		} else {
			right = append(right, data[r])
		}
	}

	// Guard against floating point rounding edge cases creating empty branches
	if len(left) == 0 || len(right) == 0 {
		(*nodes)[myIdx] = ml.IsolationTreeNode{
			FeatureIndex: -1,
			SplitValue:   0.0,
			LeftChild:    -1,
			RightChild:   -1,
			Size:         n,
		}
		return myIdx
	}

	// Recursively construct left and right child trees
	leftIdx := buildIsolationTree(left, depth+1, maxDepth, prng, nodes)
	rightIdx := buildIsolationTree(right, depth+1, maxDepth, prng, nodes)

	(*nodes)[myIdx] = ml.IsolationTreeNode{
		FeatureIndex: q,
		SplitValue:   p,
		LeftChild:    leftIdx,
		RightChild:   rightIdx,
		Size:         0,
	}

	return myIdx
}
