package ml

import (
	"fmt"
)

// CompatibilityRequirement defines the runtime constraints required by an inference engine
// to safely evaluate a model artifact.
type CompatibilityRequirement struct {
	SupportedAlgorithm     string
	SupportedFeatureSchema string
	ExpectedDimensions     int
	ExpectedMetrics        []string
	MaxAllowedTrees        int
	MaxAllowedNodesPerTree int
	MaxAllowedTotalNodes   int
	MaxAllowedTreeDepth    int
	SupportedFormatVersion string
}

// DefaultCompatibilityRequirements returns the standard requirements for the pure-Go Isolation Forest engine.
func DefaultCompatibilityRequirements() CompatibilityRequirement {
	return CompatibilityRequirement{
		SupportedAlgorithm:     AlgorithmIsolationForest,
		SupportedFeatureSchema: FeatureSchemaV1,
		ExpectedDimensions:     len(CanonicalSupportedMetrics),
		ExpectedMetrics:        CanonicalSupportedMetrics,
		MaxAllowedTrees:        MaxTrees,
		MaxAllowedNodesPerTree: MaxNodesPerTree,
		MaxAllowedTotalNodes:   MaxTotalNodes,
		MaxAllowedTreeDepth:    MaxTreeDepth,
		SupportedFormatVersion: ArtifactFormatV1,
	}
}

// CompatibilityReport captures the detailed outcome of evaluating a ModelManifest against runtime requirements.
type CompatibilityReport struct {
	Compatible           bool     `json:"compatible"`
	ModelID              string   `json:"model_id"`
	ModelVersion         string   `json:"model_version"`
	Algorithm            string   `json:"algorithm"`
	FeatureSchemaVersion string   `json:"feature_schema_version"`
	Issues               []string `json:"issues,omitempty"`
}

// CheckCompatibility evaluates whether a given ModelManifest can be loaded and executed
// by an inference implementation enforcing the specified CompatibilityRequirement.
func CheckCompatibility(manifest *ModelManifest, req CompatibilityRequirement) CompatibilityReport {
	report := CompatibilityReport{
		Compatible: true,
	}

	if manifest == nil {
		report.Compatible = false
		report.Issues = append(report.Issues, "manifest is nil")
		return report
	}

	report.ModelID = manifest.ModelID
	report.ModelVersion = manifest.ModelVersion
	report.Algorithm = manifest.Algorithm
	report.FeatureSchemaVersion = manifest.FeatureSchemaVersion

	// 1. Algorithm check
	if req.SupportedAlgorithm != "" && manifest.Algorithm != req.SupportedAlgorithm {
		report.Compatible = false
		report.Issues = append(report.Issues, fmt.Sprintf("unsupported algorithm: expected %q, got %q", req.SupportedAlgorithm, manifest.Algorithm))
	}

	// 2. Feature schema check
	if req.SupportedFeatureSchema != "" && manifest.FeatureSchemaVersion != req.SupportedFeatureSchema {
		report.Compatible = false
		report.Issues = append(report.Issues, fmt.Sprintf("unsupported feature schema: expected %q, got %q", req.SupportedFeatureSchema, manifest.FeatureSchemaVersion))
	}

	// 3. Artifact format version check (if present on manifest and requirement specified)
	if manifest.ArtifactFormatVersion != "" && req.SupportedFormatVersion != "" && manifest.ArtifactFormatVersion != req.SupportedFormatVersion {
		report.Compatible = false
		report.Issues = append(report.Issues, fmt.Sprintf("unsupported artifact format: expected %q, got %q", req.SupportedFormatVersion, manifest.ArtifactFormatVersion))
	}

	// 4. Dimensions check
	if req.ExpectedDimensions > 0 && manifest.InputDimensions != req.ExpectedDimensions {
		report.Compatible = false
		report.Issues = append(report.Issues, fmt.Sprintf("dimension mismatch: expected %d, got %d", req.ExpectedDimensions, manifest.InputDimensions))
	}

	// 5. Canonical metrics ordering check
	if len(req.ExpectedMetrics) > 0 {
		if len(manifest.SupportedMetrics) != len(req.ExpectedMetrics) {
			report.Compatible = false
			report.Issues = append(report.Issues, fmt.Sprintf("metric count mismatch: expected %d, got %d", len(req.ExpectedMetrics), len(manifest.SupportedMetrics)))
		} else {
			for i, expected := range req.ExpectedMetrics {
				if manifest.SupportedMetrics[i] != expected {
					report.Compatible = false
					report.Issues = append(report.Issues, fmt.Sprintf("metric ordering mismatch at index %d: expected %q, got %q", i, expected, manifest.SupportedMetrics[i]))
				}
			}
		}
	}

	// 6. Resource constraint checks
	if req.MaxAllowedTrees > 0 && len(manifest.Trees) > req.MaxAllowedTrees {
		report.Compatible = false
		report.Issues = append(report.Issues, fmt.Sprintf("tree count %d exceeds maximum allowed %d", len(manifest.Trees), req.MaxAllowedTrees))
	}

	totalNodes := 0
	for treeIdx, tree := range manifest.Trees {
		nodeCount := len(tree.Nodes)
		if req.MaxAllowedNodesPerTree > 0 && nodeCount > req.MaxAllowedNodesPerTree {
			report.Compatible = false
			report.Issues = append(report.Issues, fmt.Sprintf("tree %d node count %d exceeds limit %d", treeIdx, nodeCount, req.MaxAllowedNodesPerTree))
		}
		totalNodes += nodeCount
	}
	if req.MaxAllowedTotalNodes > 0 && totalNodes > req.MaxAllowedTotalNodes {
		report.Compatible = false
		report.Issues = append(report.Issues, fmt.Sprintf("cumulative node count %d exceeds limit %d", totalNodes, req.MaxAllowedTotalNodes))
	}

	return report
}
