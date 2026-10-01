package ml

import (
	"testing"
)

func TestCompatibility_DefaultRequirements(t *testing.T) {
	manifest := helperValidManifest()
	req := DefaultCompatibilityRequirements()

	report := CheckCompatibility(manifest, req)
	if !report.Compatible {
		t.Fatalf("expected manifest to be compatible, got issues: %v", report.Issues)
	}
	if len(report.Issues) != 0 {
		t.Fatalf("expected zero issues, got %d", len(report.Issues))
	}
}

func TestCompatibility_NilManifest(t *testing.T) {
	req := DefaultCompatibilityRequirements()
	report := CheckCompatibility(nil, req)
	if report.Compatible {
		t.Fatalf("expected nil manifest to be incompatible")
	}
}

func TestCompatibility_AlgorithmMismatch(t *testing.T) {
	manifest := helperValidManifest()
	manifest.Algorithm = "unsupported_alg"
	req := DefaultCompatibilityRequirements()

	report := CheckCompatibility(manifest, req)
	if report.Compatible {
		t.Fatalf("expected incompatible report on algorithm mismatch")
	}
	if len(report.Issues) == 0 {
		t.Fatalf("expected issues listed in report")
	}
}

func TestCompatibility_FeatureSchemaMismatch(t *testing.T) {
	manifest := helperValidManifest()
	manifest.FeatureSchemaVersion = "features.v2.0.0"
	req := DefaultCompatibilityRequirements()

	report := CheckCompatibility(manifest, req)
	if report.Compatible {
		t.Fatalf("expected incompatible report on feature schema mismatch")
	}
}

func TestCompatibility_DimensionMismatch(t *testing.T) {
	manifest := helperValidManifest()
	manifest.InputDimensions = 6
	req := DefaultCompatibilityRequirements()

	report := CheckCompatibility(manifest, req)
	if report.Compatible {
		t.Fatalf("expected incompatible report on dimension mismatch")
	}
}

func TestCompatibility_ResourceConstraintBreach(t *testing.T) {
	manifest := helperValidManifest()
	req := DefaultCompatibilityRequirements()
	req.MaxAllowedTrees = 0                                    // test with custom lower limit
	req.MaxAllowedTrees = 1                                    // manifest has 1 tree, let's set requirement to 0 or manifest to 2
	manifest.Trees = append(manifest.Trees, manifest.Trees[0]) // 2 trees

	report := CheckCompatibility(manifest, req)
	if report.Compatible {
		t.Fatalf("expected incompatible report when tree count exceeds requirement")
	}
}
