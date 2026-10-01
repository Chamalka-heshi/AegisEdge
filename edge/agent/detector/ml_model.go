package detector

import "github.com/Chamalka-heshi/AegisEdge/shared/ml"

// Type aliases for shared ML model contracts.
// Preserves complete backward compatibility for callers and tests expecting detector.* types.
type (
	ModelStatus                = ml.ModelStatus
	FeatureNormalizationParams = ml.FeatureNormalizationParams
	IsolationTreeNode          = ml.IsolationTreeNode
	IsolationTree              = ml.IsolationTree
	SignatureMetadata          = ml.SignatureMetadata
	ModelManifest              = ml.ModelManifest
	MLModelManifest            = ml.ModelManifest
)

// Re-exported lifecycle statuses.
const (
	ModelStatusActive   = ml.ModelStatusActive
	ModelStatusStale    = ml.ModelStatusStale
	ModelStatusDisabled = ml.ModelStatusDisabled
)

// Re-exported canonical identifiers, format tags, and mathematical constants.
const (
	AlgorithmIsolationForest = ml.AlgorithmIsolationForest
	FeatureSchemaV1          = ml.FeatureSchemaV1
	ArtifactFormatV1         = ml.ArtifactFormatV1
	EulerMascheroni          = ml.EulerMascheroni
)

// Re-exported resource safety limits.
const (
	MaxTrees           = ml.MaxTrees
	MaxNodesPerTree    = ml.MaxNodesPerTree
	MaxTotalNodes      = ml.MaxTotalNodes
	MaxInputDimensions = ml.MaxInputDimensions
	MaxSubSampleSize   = ml.MaxSubSampleSize
	MaxTreeDepth       = ml.MaxTreeDepth
)

// Re-exported domain errors.
var (
	ErrNilModelManifest           = ml.ErrNilModelManifest
	ErrEmptyModelID               = ml.ErrEmptyModelID
	ErrEmptyModelVersion          = ml.ErrEmptyModelVersion
	ErrUnsupportedAlgorithm       = ml.ErrUnsupportedAlgorithm
	ErrUnsupportedFeatureSchema   = ml.ErrUnsupportedFeatureSchema
	ErrInvalidInputDimensions     = ml.ErrInvalidInputDimensions
	ErrUnsupportedMetricName      = ml.ErrUnsupportedMetricName
	ErrDuplicateMetricName        = ml.ErrDuplicateMetricName
	ErrInvalidNormalizationParams = ml.ErrInvalidNormalizationParams
	ErrInvalidDecisionThreshold   = ml.ErrInvalidDecisionThreshold
	ErrInvalidSubSampleSize       = ml.ErrInvalidSubSampleSize
	ErrInvalidModelStatus         = ml.ErrInvalidModelStatus
	ErrChecksumMismatch           = ml.ErrChecksumMismatch
	ErrEmptyTrees                 = ml.ErrEmptyTrees
	ErrExcessiveTreeCount         = ml.ErrExcessiveTreeCount
	ErrExcessiveNodeCount         = ml.ErrExcessiveNodeCount
	ErrExcessiveTreeDepth         = ml.ErrExcessiveTreeDepth
	ErrInvalidTreeRoot            = ml.ErrInvalidTreeRoot
	ErrInvalidChildIndex          = ml.ErrInvalidChildIndex
	ErrInvalidFeatureIndex        = ml.ErrInvalidFeatureIndex
	ErrInvalidSplitValue          = ml.ErrInvalidSplitValue
	ErrMalformedLeafNode          = ml.ErrMalformedLeafNode
	ErrTreeCycleDetected          = ml.ErrTreeCycleDetected
	ErrUnreachableNodes           = ml.ErrUnreachableNodes
	ErrMissingRequiredFeature     = ml.ErrMissingRequiredFeature
	ErrInvalidFeatureValue        = ml.ErrInvalidFeatureValue
	ErrDimensionMismatch          = ml.ErrDimensionMismatch
	ErrModelNotActive             = ml.ErrModelNotActive
)

// Re-exported canonical variables and functions.
var (
	CanonicalSupportedMetrics = ml.CanonicalSupportedMetrics
	ExpectedPathLength        = ml.ExpectedPathLength
)
