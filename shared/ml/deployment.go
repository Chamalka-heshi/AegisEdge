package ml

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// DeploymentState represents the lifecycle state of distributing and deploying a model to an edge node.
//
// Conceptual Lifecycle:
//
//	PENDING ──> DOWNLOADING ──> VERIFIED ──> VALIDATED ──> STAGED ──> ACTIVE
//	   │             │              │            │           │        │
//	   ▼             ▼              ▼            ▼           ▼        ▼
//	FAILED        FAILED         FAILED       FAILED      FAILED   ROLLED_BACK
//	   │             │              │            │           │        │
//	   ▼             ▼              ▼            ▼           ▼        ▼
//	REJECTED      REJECTED       REJECTED     REJECTED    REJECTED    FAILED
type DeploymentState string

const (
	DeploymentStatePending     DeploymentState = "PENDING"
	DeploymentStateDownloading DeploymentState = "DOWNLOADING"
	DeploymentStateVerified    DeploymentState = "VERIFIED"
	DeploymentStateValidated   DeploymentState = "VALIDATED"
	DeploymentStateStaged      DeploymentState = "STAGED"
	DeploymentStateActive      DeploymentState = "ACTIVE"
	DeploymentStateRejected    DeploymentState = "REJECTED"
	DeploymentStateFailed      DeploymentState = "FAILED"
	DeploymentStateRolledBack  DeploymentState = "ROLLED_BACK"
)

// Deployment domain errors.
var (
	ErrEmptyDeploymentID      = errors.New("deployment_id cannot be empty")
	ErrInvalidArtifactSize    = errors.New("artifact_size_bytes must be strictly positive")
	ErrInvalidDeploymentState = errors.New("unknown or invalid deployment state")
	ErrInvalidStateTransition = errors.New("invalid deployment state transition")
)

// validDeploymentTransitions defines allowed state transitions in the deployment lifecycle.
var validDeploymentTransitions = map[DeploymentState][]DeploymentState{
	DeploymentStatePending: {
		DeploymentStateDownloading,
		DeploymentStateRejected,
		DeploymentStateFailed,
	},
	DeploymentStateDownloading: {
		DeploymentStateVerified,
		DeploymentStateFailed,
		DeploymentStateRejected,
	},
	DeploymentStateVerified: {
		DeploymentStateValidated,
		DeploymentStateRejected,
		DeploymentStateFailed,
	},
	DeploymentStateValidated: {
		DeploymentStateStaged,
		DeploymentStateRejected,
		DeploymentStateFailed,
	},
	DeploymentStateStaged: {
		DeploymentStateActive,
		DeploymentStateRejected,
		DeploymentStateFailed,
	},
	DeploymentStateActive: {
		DeploymentStateRolledBack,
		DeploymentStateFailed,
	},
	DeploymentStateRolledBack: {}, // terminal state
	DeploymentStateRejected:   {}, // terminal state
	DeploymentStateFailed:     {}, // terminal state
}

// IsValidDeploymentState returns true if the state is recognized.
func IsValidDeploymentState(s DeploymentState) bool {
	switch s {
	case DeploymentStatePending,
		DeploymentStateDownloading,
		DeploymentStateVerified,
		DeploymentStateValidated,
		DeploymentStateStaged,
		DeploymentStateActive,
		DeploymentStateRejected,
		DeploymentStateFailed,
		DeploymentStateRolledBack:
		return true
	default:
		return false
	}
}

// CanTransitionDeploymentState checks whether transitioning from currentState to targetState
// is permitted by the canonical deployment lifecycle state machine.
func CanTransitionDeploymentState(from, to DeploymentState) bool {
	allowed, exists := validDeploymentTransitions[from]
	if !exists {
		return false
	}
	for _, candidate := range allowed {
		if candidate == to {
			return true
		}
	}
	return false
}

// DeploymentMetadata defines the payload exchanged between control plane and edge node
// when distributing a model artifact. It is decoupled from telemetry and storage schemas.
type DeploymentMetadata struct {
	DeploymentID          string             `json:"deployment_id"`
	ModelID               string             `json:"model_id"`
	ModelVersion          string             `json:"model_version"`
	ArtifactFormatVersion string             `json:"artifact_format_version,omitempty"`
	FeatureSchemaVersion  string             `json:"feature_schema_version"`
	ChecksumSHA256        string             `json:"checksum_sha256"`
	ArtifactSizeBytes     int64              `json:"artifact_size_bytes"`
	DeployedAt            time.Time          `json:"deployed_at"`
	RollbackAuthorized    bool               `json:"rollback_authorized"`
	TargetFleet           string             `json:"target_fleet,omitempty"`
	TargetNodeID          string             `json:"target_node_id,omitempty"`
	SignatureInfo         *SignatureMetadata `json:"signature_info,omitempty"`
}

// Validate verifies the deployment metadata contract.
func (d *DeploymentMetadata) Validate() error {
	if strings.TrimSpace(d.DeploymentID) == "" {
		return ErrEmptyDeploymentID
	}
	if strings.TrimSpace(d.ModelID) == "" {
		return ErrEmptyModelID
	}
	if strings.TrimSpace(d.ModelVersion) == "" {
		return ErrEmptyModelVersion
	}
	if d.FeatureSchemaVersion != FeatureSchemaV1 {
		return fmt.Errorf("%w: got %q", ErrUnsupportedFeatureSchema, d.FeatureSchemaVersion)
	}
	if strings.TrimSpace(d.ChecksumSHA256) == "" {
		return ErrChecksumMismatch
	}
	if d.ArtifactSizeBytes <= 0 {
		return ErrInvalidArtifactSize
	}
	if d.DeployedAt.IsZero() {
		return errors.New("deployed_at timestamp must be non-zero")
	}
	return nil
}
