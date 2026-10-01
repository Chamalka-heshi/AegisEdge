package ml

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDeploymentState_Validation(t *testing.T) {
	validStates := []DeploymentState{
		DeploymentStatePending,
		DeploymentStateDownloading,
		DeploymentStateVerified,
		DeploymentStateValidated,
		DeploymentStateStaged,
		DeploymentStateActive,
		DeploymentStateRejected,
		DeploymentStateFailed,
		DeploymentStateRolledBack,
	}

	for _, s := range validStates {
		if !IsValidDeploymentState(s) {
			t.Errorf("expected state %q to be valid", s)
		}
	}

	if IsValidDeploymentState("UNKNOWN_STATE") {
		t.Errorf("expected UNKNOWN_STATE to be invalid")
	}
}

func TestDeploymentState_Transitions(t *testing.T) {
	// 1. Happy path: PENDING -> DOWNLOADING -> VERIFIED -> VALIDATED -> STAGED -> ACTIVE
	happyPath := []struct {
		from DeploymentState
		to   DeploymentState
	}{
		{DeploymentStatePending, DeploymentStateDownloading},
		{DeploymentStateDownloading, DeploymentStateVerified},
		{DeploymentStateVerified, DeploymentStateValidated},
		{DeploymentStateValidated, DeploymentStateStaged},
		{DeploymentStateStaged, DeploymentStateActive},
		{DeploymentStateActive, DeploymentStateRolledBack},
	}

	for _, step := range happyPath {
		if !CanTransitionDeploymentState(step.from, step.to) {
			t.Errorf("expected transition %s -> %s to be allowed", step.from, step.to)
		}
	}

	// 2. Failure transitions from intermediate states
	failureTransitions := []struct {
		from DeploymentState
		to   DeploymentState
	}{
		{DeploymentStatePending, DeploymentStateFailed},
		{DeploymentStatePending, DeploymentStateRejected},
		{DeploymentStateDownloading, DeploymentStateFailed},
		{DeploymentStateVerified, DeploymentStateRejected},
		{DeploymentStateValidated, DeploymentStateRejected},
		{DeploymentStateStaged, DeploymentStateRejected},
		{DeploymentStateActive, DeploymentStateFailed},
	}

	for _, step := range failureTransitions {
		if !CanTransitionDeploymentState(step.from, step.to) {
			t.Errorf("expected failure transition %s -> %s to be allowed", step.from, step.to)
		}
	}

	// 3. Illegal transitions
	illegalTransitions := []struct {
		from DeploymentState
		to   DeploymentState
	}{
		{DeploymentStatePending, DeploymentStateActive},       // skipping stages
		{DeploymentStatePending, DeploymentStateStaged},       // skipping stages
		{DeploymentStateRolledBack, DeploymentStateActive},    // terminal state
		{DeploymentStateRejected, DeploymentStateDownloading}, // terminal state
		{DeploymentStateFailed, DeploymentStateActive},        // terminal state
	}

	for _, step := range illegalTransitions {
		if CanTransitionDeploymentState(step.from, step.to) {
			t.Errorf("expected transition %s -> %s to be disallowed", step.from, step.to)
		}
	}
}

func TestDeploymentMetadata_Validation(t *testing.T) {
	meta := DeploymentMetadata{
		DeploymentID:          "deploy-001",
		ModelID:               "iforest-v1",
		ModelVersion:          "1.0.0",
		ArtifactFormatVersion: ArtifactFormatV1,
		FeatureSchemaVersion:  FeatureSchemaV1,
		ChecksumSHA256:        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		ArtifactSizeBytes:     1024,
		DeployedAt:            time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		RollbackAuthorized:    false,
		TargetFleet:           "production-gateways",
	}

	if err := meta.Validate(); err != nil {
		t.Fatalf("expected valid deployment metadata, got error: %v", err)
	}

	// Test missing deployment ID
	invalid := meta
	invalid.DeploymentID = ""
	if err := invalid.Validate(); err == nil {
		t.Fatalf("expected error on empty deployment ID")
	}

	// Test negative artifact size
	invalidSize := meta
	invalidSize.ArtifactSizeBytes = 0
	if err := invalidSize.Validate(); err == nil {
		t.Fatalf("expected error on zero artifact size")
	}

	// Test unsupported schema
	invalidSchema := meta
	invalidSchema.FeatureSchemaVersion = "features.v2.0.0"
	if err := invalidSchema.Validate(); err == nil {
		t.Fatalf("expected error on unsupported feature schema")
	}
}

func TestDeploymentMetadata_JSONRoundTrip(t *testing.T) {
	meta := DeploymentMetadata{
		DeploymentID:          "deploy-002",
		ModelID:               "iforest-v1",
		ModelVersion:          "1.1.0",
		ArtifactFormatVersion: ArtifactFormatV1,
		FeatureSchemaVersion:  FeatureSchemaV1,
		ChecksumSHA256:        "abc123def456",
		ArtifactSizeBytes:     2048,
		DeployedAt:            time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
		RollbackAuthorized:    true,
	}

	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("json marshal failed: %v", err)
	}

	var decoded DeploymentMetadata
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json unmarshal failed: %v", err)
	}

	if decoded.DeploymentID != meta.DeploymentID || decoded.RollbackAuthorized != meta.RollbackAuthorized {
		t.Fatalf("mismatch in decoded metadata: %+v", decoded)
	}
}
