package detector

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// ModelStatus represents the lifecycle state of a deployed model artifact.
type ModelStatus string

const (
	ModelStatusActive   ModelStatus = "ACTIVE"
	ModelStatusStale    ModelStatus = "STALE"
	ModelStatusDisabled ModelStatus = "DISABLED"
)

// Canonical identifiers and mathematical constants.
const (
	AlgorithmIsolationForest = "isolation_forest"
	FeatureSchemaV1          = "features.v1.0.0"

	// EulerMascheroni is the Euler-Mascheroni constant gamma used in harmonic number approximations
	// for the standard Isolation Forest expected path-length normalization factor c(n).
	EulerMascheroni = 0.577215664901532860606512090082402431042
)

// Resource safety limits.
//
// These limits prevent denial-of-service, excessive memory consumption,
// unbounded recursion/traversal, and arithmetic overflow during model deserialization.
const (
	// MaxTrees bounds the maximum number of isolation trees in a model manifest.
	// Rationale: Prevents excessive CPU consumption and latency spikes during sequential tree traversal.
	MaxTrees = 1000

	// MaxNodesPerTree bounds the node count within a single tree.
	// Rationale: For standard subsample sizes (psi <= 1024), tree depth is bounded by ~ceil(log2(psi)) <= 10,
	// yielding <= 2047 total nodes. 2048 prevents unbounded node allocations per tree.
	MaxNodesPerTree = 2048

	// MaxTotalNodes bounds the cumulative node count across all trees in the manifest.
	// Rationale: Guarantees strict upper bound on overall heap consumption for model data structures.
	MaxTotalNodes = 100000

	// MaxInputDimensions bounds the dimension count of the input feature vector.
	// Rationale: Ensures feature vectors and normalization metadata remain compact and cache-friendly.
	MaxInputDimensions = 64

	// MaxSubSampleSize bounds the subsample size parameter psi.
	// Rationale: Bounds subsample scaling factor c(psi) and avoids numeric instability in path-length equations.
	MaxSubSampleSize = 10000

	// MaxTreeDepth bounds the maximum path depth permitted when traversing a decision tree.
	// Rationale: Guarantees traversal completes in bounded time and prevents infinite loops on deep trees.
	MaxTreeDepth = 50
)

// Model validation and inference domain errors.
var (
	ErrNilModelManifest           = errors.New("model manifest cannot be nil")
	ErrEmptyModelID               = errors.New("model_id cannot be empty")
	ErrEmptyModelVersion          = errors.New("model_version cannot be empty")
	ErrUnsupportedAlgorithm       = errors.New("unsupported algorithm: must be 'isolation_forest'")
	ErrUnsupportedFeatureSchema   = errors.New("unsupported feature_schema_version: must be 'features.v1.0.0'")
	ErrInvalidInputDimensions     = errors.New("input_dimensions must match supported_metrics count and be positive")
	ErrUnsupportedMetricName      = errors.New("unsupported metric name in feature schema")
	ErrDuplicateMetricName        = errors.New("duplicate metric name in feature schema")
	ErrInvalidNormalizationParams = errors.New("invalid feature normalization parameters")
	ErrInvalidDecisionThreshold   = errors.New("decision_threshold must be a finite number in (0.0, 1.0)")
	ErrInvalidSubSampleSize       = errors.New("sub_sample_size must be >= 2 and <= MaxSubSampleSize")
	ErrInvalidModelStatus         = errors.New("model status must be ACTIVE")
	ErrChecksumMismatch           = errors.New("model manifest SHA-256 checksum mismatch")
	ErrEmptyTrees                 = errors.New("model must contain at least one isolation tree")
	ErrExcessiveTreeCount         = errors.New("tree count exceeds safety limit")
	ErrExcessiveNodeCount         = errors.New("node count exceeds safety limit")
	ErrExcessiveTreeDepth         = errors.New("tree depth exceeds safety limit")
	ErrInvalidTreeRoot            = errors.New("tree root index is invalid")
	ErrInvalidChildIndex          = errors.New("invalid tree child node index")
	ErrInvalidFeatureIndex        = errors.New("invalid node feature index")
	ErrInvalidSplitValue          = errors.New("split value must be a finite number (not NaN or Inf)")
	ErrMalformedLeafNode          = errors.New("malformed leaf node")
	ErrTreeCycleDetected          = errors.New("cycle detected in isolation tree")
	ErrUnreachableNodes           = errors.New("unreachable nodes detected in isolation tree")
	ErrMissingRequiredFeature     = errors.New("missing required telemetry metric in feature vector")
	ErrInvalidFeatureValue        = errors.New("feature value must be a finite number (not NaN or Inf)")
	ErrDimensionMismatch          = errors.New("feature vector dimensions do not match model schema")
	ErrModelNotActive             = errors.New("model is not active for inference")
)

// CanonicalSupportedMetrics defines the required 4-dimensional telemetry metrics in canonical order.
var CanonicalSupportedMetrics = []string{
	"cpu_usage_percent",
	"memory_usage_percent",
	"disk_usage_percent",
	"temperature_celsius",
}

// FeatureNormalizationParams stores immutable normalization scaling parameters computed during offline training.
type FeatureNormalizationParams struct {
	MetricName string  `json:"metric_name"`
	Mean       float64 `json:"mean"`
	StdDev     float64 `json:"std_dev"`
	Min        float64 `json:"min"`
	Max        float64 `json:"max"`
}

// IsolationTreeNode represents a single node in a pure Go compiled decision tree.
//
// Internal nodes define a split condition:
//   - FeatureIndex: [0, InputDimensions-1]
//   - SplitValue: finite float threshold
//   - LeftChild: non-negative index in tree's Nodes slice
//   - RightChild: non-negative index in tree's Nodes slice (LeftChild != RightChild)
//
// Leaf nodes define terminal isolation states:
//   - FeatureIndex: -1
//   - LeftChild: -1
//   - RightChild: -1
//   - Size: >= 1 (number of training samples terminating at this leaf)
type IsolationTreeNode struct {
	FeatureIndex int     `json:"feature_index"`
	SplitValue   float64 `json:"split_value"`
	LeftChild    int     `json:"left_child"`
	RightChild   int     `json:"right_child"`
	Size         int     `json:"size"`
}

// IsLeaf returns true if this node is a terminal leaf node.
func (n *IsolationTreeNode) IsLeaf() bool {
	return n.LeftChild == -1 && n.RightChild == -1
}

// IsolationTree represents a single isolation decision tree stored as a flat slice of nodes.
type IsolationTree struct {
	RootIndex int                 `json:"root_index"`
	Nodes     []IsolationTreeNode `json:"nodes"`
}

// MLModelManifest defines the self-contained, serializable model artifact evaluated by MLDetector.
// It is completely decoupled from operational telemetry and incident schemas.
type MLModelManifest struct {
	ModelID              string                       `json:"model_id"`
	ModelVersion         string                       `json:"model_version"`
	Algorithm            string                       `json:"algorithm"`
	FeatureSchemaVersion string                       `json:"feature_schema_version"`
	TrainingDatasetID    string                       `json:"training_dataset_id,omitempty"`
	CreatedAt            time.Time                    `json:"created_at"`
	InputDimensions      int                          `json:"input_dimensions"`
	SupportedMetrics     []string                     `json:"supported_metrics"`
	NormalizationParams  []FeatureNormalizationParams `json:"normalization_params"`
	Trees                []IsolationTree              `json:"trees"`
	SubSampleSize        int                          `json:"sub_sample_size"`
	DecisionThreshold    float64                      `json:"decision_threshold"`
	ChecksumSHA256       string                       `json:"checksum_sha256,omitempty"`
	Status               ModelStatus                  `json:"status"`
}

// ExpectedPathLength calculates the standard Isolation Forest expected path-length
// of an unsuccessful search in a Binary Search Tree (BST) built over n samples:
//
//	c(n) = 2 * (ln(n - 1) + EulerMascheroni) - 2 * (n - 1) / n
//
// Standard base cases:
//   - n <= 1: c(n) = 0.0 (an isolated sample requires 0 edges)
//   - n == 2: c(n) = 1.0 (two samples require 1 split edge)
//
// Reference: Liu, Ting, Zhou (2008), "Isolation Forest", IEEE ICDM.
func ExpectedPathLength(n float64) float64 {
	if n <= 1.0 {
		return 0.0
	}
	if n == 2.0 {
		return 1.0
	}
	return 2.0*(math.Log(n-1.0)+EulerMascheroni) - (2.0 * (n - 1.0) / n)
}

// ComputeChecksum generates a deterministic SHA-256 hexadecimal checksum across the model manifest,
// excluding the ChecksumSHA256 field itself.
func (m *MLModelManifest) ComputeChecksum() (string, error) {
	if m == nil {
		return "", ErrNilModelManifest
	}

	clone := *m
	clone.ChecksumSHA256 = ""

	data, err := json.Marshal(clone)
	if err != nil {
		return "", fmt.Errorf("failed to marshal model manifest for checksum: %w", err)
	}

	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum), nil
}

// Clone creates a deep copy of the model manifest to guarantee caller-side isolation.
func (m *MLModelManifest) Clone() *MLModelManifest {
	if m == nil {
		return nil
	}

	cp := *m
	if m.SupportedMetrics != nil {
		cp.SupportedMetrics = make([]string, len(m.SupportedMetrics))
		copy(cp.SupportedMetrics, m.SupportedMetrics)
	}
	if m.NormalizationParams != nil {
		cp.NormalizationParams = make([]FeatureNormalizationParams, len(m.NormalizationParams))
		copy(cp.NormalizationParams, m.NormalizationParams)
	}
	if m.Trees != nil {
		cp.Trees = make([]IsolationTree, len(m.Trees))
		for i, t := range m.Trees {
			treeCp := IsolationTree{
				RootIndex: t.RootIndex,
				Nodes:     make([]IsolationTreeNode, len(t.Nodes)),
			}
			copy(treeCp.Nodes, t.Nodes)
			cp.Trees[i] = treeCp
		}
	}

	return &cp
}

// Validate performs comprehensive, strict validation of an MLModelManifest before inference is permitted.
// Any structural, numerical, or integrity defect results in an explicit error.
func (m *MLModelManifest) Validate() error {
	if m == nil {
		return ErrNilModelManifest
	}

	// 1. Basic Metadata
	if strings.TrimSpace(m.ModelID) == "" {
		return ErrEmptyModelID
	}
	if strings.TrimSpace(m.ModelVersion) == "" {
		return ErrEmptyModelVersion
	}
	if m.Algorithm != AlgorithmIsolationForest {
		return fmt.Errorf("%w: got %q", ErrUnsupportedAlgorithm, m.Algorithm)
	}
	if m.FeatureSchemaVersion != FeatureSchemaV1 {
		return fmt.Errorf("%w: got %q", ErrUnsupportedFeatureSchema, m.FeatureSchemaVersion)
	}
	if m.Status != ModelStatusActive {
		return fmt.Errorf("%w: status is %q", ErrInvalidModelStatus, m.Status)
	}

	// 2. Dimensions & Supported Metrics
	if m.InputDimensions <= 0 || m.InputDimensions > MaxInputDimensions {
		return fmt.Errorf("%w: input_dimensions %d out of bounds (1..%d)", ErrInvalidInputDimensions, m.InputDimensions, MaxInputDimensions)
	}
	if len(m.SupportedMetrics) != m.InputDimensions {
		return fmt.Errorf("%w: len(supported_metrics)=%d != input_dimensions=%d", ErrInvalidInputDimensions, len(m.SupportedMetrics), m.InputDimensions)
	}

	metricSet := make(map[string]struct{}, len(m.SupportedMetrics))
	for i, name := range m.SupportedMetrics {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			return fmt.Errorf("%w: empty metric at index %d", ErrUnsupportedMetricName, i)
		}
		if _, exists := metricSet[trimmed]; exists {
			return fmt.Errorf("%w: duplicate metric %q", ErrDuplicateMetricName, trimmed)
		}
		metricSet[trimmed] = struct{}{}

		// Verify against canonical metrics for FeatureSchemaV1
		var isCanonical bool
		for _, canon := range CanonicalSupportedMetrics {
			if trimmed == canon {
				isCanonical = true
				break
			}
		}
		if !isCanonical {
			return fmt.Errorf("%w: %q not in canonical schema", ErrUnsupportedMetricName, trimmed)
		}
	}

	// 3. Normalization Parameters
	if len(m.NormalizationParams) != m.InputDimensions {
		return fmt.Errorf("%w: normalization_params count (%d) must match input_dimensions (%d)",
			ErrInvalidNormalizationParams, len(m.NormalizationParams), m.InputDimensions)
	}
	for i, norm := range m.NormalizationParams {
		if norm.MetricName != m.SupportedMetrics[i] {
			return fmt.Errorf("%w: index %d metric %q does not match supported_metrics %q",
				ErrInvalidNormalizationParams, i, norm.MetricName, m.SupportedMetrics[i])
		}
		if math.IsNaN(norm.Mean) || math.IsInf(norm.Mean, 0) {
			return fmt.Errorf("%w: metric %q has non-finite mean", ErrInvalidNormalizationParams, norm.MetricName)
		}
		if math.IsNaN(norm.StdDev) || math.IsInf(norm.StdDev, 0) || norm.StdDev <= 0 {
			return fmt.Errorf("%w: metric %q has invalid stddev (must be finite positive, got %f)",
				ErrInvalidNormalizationParams, norm.MetricName, norm.StdDev)
		}
		if math.IsNaN(norm.Min) || math.IsInf(norm.Min, 0) {
			return fmt.Errorf("%w: metric %q has non-finite min", ErrInvalidNormalizationParams, norm.MetricName)
		}
		if math.IsNaN(norm.Max) || math.IsInf(norm.Max, 0) {
			return fmt.Errorf("%w: metric %q has non-finite max", ErrInvalidNormalizationParams, norm.MetricName)
		}
		if norm.Min > norm.Max {
			return fmt.Errorf("%w: metric %q has min (%f) > max (%f)",
				ErrInvalidNormalizationParams, norm.MetricName, norm.Min, norm.Max)
		}
	}

	// 4. Decision Threshold & SubSampleSize
	if math.IsNaN(m.DecisionThreshold) || math.IsInf(m.DecisionThreshold, 0) || m.DecisionThreshold <= 0.0 || m.DecisionThreshold >= 1.0 {
		return fmt.Errorf("%w: must be in (0.0, 1.0), got %f", ErrInvalidDecisionThreshold, m.DecisionThreshold)
	}
	if m.SubSampleSize < 2 || m.SubSampleSize > MaxSubSampleSize {
		return fmt.Errorf("%w: must be in [2, %d], got %d", ErrInvalidSubSampleSize, MaxSubSampleSize, m.SubSampleSize)
	}

	// 5. Trees & Nodes Validation
	if len(m.Trees) == 0 {
		return ErrEmptyTrees
	}
	if len(m.Trees) > MaxTrees {
		return fmt.Errorf("%w: tree count %d exceeds maximum %d", ErrExcessiveTreeCount, len(m.Trees), MaxTrees)
	}

	totalNodes := 0
	for treeIdx, tree := range m.Trees {
		nodeCount := len(tree.Nodes)
		if nodeCount == 0 {
			return fmt.Errorf("%w: tree %d contains 0 nodes", ErrEmptyTrees, treeIdx)
		}
		if nodeCount > MaxNodesPerTree {
			return fmt.Errorf("%w: tree %d contains %d nodes (max %d)", ErrExcessiveNodeCount, treeIdx, nodeCount, MaxNodesPerTree)
		}
		totalNodes += nodeCount
		if totalNodes > MaxTotalNodes {
			return fmt.Errorf("%w: cumulative nodes %d exceeds total limit %d", ErrExcessiveNodeCount, totalNodes, MaxTotalNodes)
		}

		if tree.RootIndex < 0 || tree.RootIndex >= nodeCount {
			return fmt.Errorf("%w: tree %d has invalid root index %d for node count %d",
				ErrInvalidTreeRoot, treeIdx, tree.RootIndex, nodeCount)
		}

		// Validate each node structure
		for nodeIdx, node := range tree.Nodes {
			if node.IsLeaf() {
				// Leaf node validation
				if node.FeatureIndex != -1 {
					return fmt.Errorf("%w: leaf node %d in tree %d has feature_index %d (must be -1)",
						ErrMalformedLeafNode, nodeIdx, treeIdx, node.FeatureIndex)
				}
				if node.Size < 1 {
					return fmt.Errorf("%w: leaf node %d in tree %d has invalid size %d (must be >= 1)",
						ErrMalformedLeafNode, nodeIdx, treeIdx, node.Size)
				}
				if math.IsNaN(node.SplitValue) || math.IsInf(node.SplitValue, 0) {
					return fmt.Errorf("%w: leaf node %d in tree %d has non-finite split value",
						ErrInvalidSplitValue, nodeIdx, treeIdx)
				}
			} else {
				// Internal node validation
				if node.LeftChild < 0 || node.LeftChild >= nodeCount {
					return fmt.Errorf("%w: internal node %d in tree %d has invalid left_child %d",
						ErrInvalidChildIndex, nodeIdx, treeIdx, node.LeftChild)
				}
				if node.RightChild < 0 || node.RightChild >= nodeCount {
					return fmt.Errorf("%w: internal node %d in tree %d has invalid right_child %d",
						ErrInvalidChildIndex, nodeIdx, treeIdx, node.RightChild)
				}
				if node.LeftChild == node.RightChild {
					return fmt.Errorf("%w: internal node %d in tree %d has identical left and right child (%d)",
						ErrInvalidChildIndex, nodeIdx, treeIdx, node.LeftChild)
				}
				if node.FeatureIndex < 0 || node.FeatureIndex >= m.InputDimensions {
					return fmt.Errorf("%w: internal node %d in tree %d has feature_index %d out of bounds (0..%d)",
						ErrInvalidFeatureIndex, nodeIdx, treeIdx, node.FeatureIndex, m.InputDimensions-1)
				}
				if math.IsNaN(node.SplitValue) || math.IsInf(node.SplitValue, 0) {
					return fmt.Errorf("%w: internal node %d in tree %d has non-finite split value",
						ErrInvalidSplitValue, nodeIdx, treeIdx)
				}
			}
		}

		// Tree Graph Verification: acyclicity, single parent, bounded depth, and full reachability
		inDegree := make([]int, nodeCount)
		visited := make([]bool, nodeCount)

		type stackEntry struct {
			index int
			depth int
		}

		stack := []stackEntry{{index: tree.RootIndex, depth: 0}}
		visited[tree.RootIndex] = true

		for len(stack) > 0 {
			curr := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			if curr.depth > MaxTreeDepth {
				return fmt.Errorf("%w: tree %d depth %d exceeds maximum depth %d",
					ErrExcessiveTreeDepth, treeIdx, curr.depth, MaxTreeDepth)
			}

			node := tree.Nodes[curr.index]
			if !node.IsLeaf() {
				// Left child
				inDegree[node.LeftChild]++
				if inDegree[node.LeftChild] > 1 || visited[node.LeftChild] {
					return fmt.Errorf("%w: node %d in tree %d reached via multiple paths or cycle",
						ErrTreeCycleDetected, node.LeftChild, treeIdx)
				}
				visited[node.LeftChild] = true
				stack = append(stack, stackEntry{index: node.LeftChild, depth: curr.depth + 1})

				// Right child
				inDegree[node.RightChild]++
				if inDegree[node.RightChild] > 1 || visited[node.RightChild] {
					return fmt.Errorf("%w: node %d in tree %d reached via multiple paths or cycle",
						ErrTreeCycleDetected, node.RightChild, treeIdx)
				}
				visited[node.RightChild] = true
				stack = append(stack, stackEntry{index: node.RightChild, depth: curr.depth + 1})
			}
		}

		// Verify that all nodes in the tree slice are reachable from the root
		for i, reached := range visited {
			if !reached {
				return fmt.Errorf("%w: node %d in tree %d is unreachable from root %d",
					ErrUnreachableNodes, i, treeIdx, tree.RootIndex)
			}
		}
	}

	// 6. Cryptographic Integrity Verification (if ChecksumSHA256 is present)
	if strings.TrimSpace(m.ChecksumSHA256) != "" {
		expectedSum, err := m.ComputeChecksum()
		if err != nil {
			return fmt.Errorf("failed to compute manifest checksum: %w", err)
		}
		if !strings.EqualFold(strings.TrimSpace(m.ChecksumSHA256), expectedSum) {
			return fmt.Errorf("%w: expected %s, got %s", ErrChecksumMismatch, expectedSum, m.ChecksumSHA256)
		}
	}

	return nil
}
