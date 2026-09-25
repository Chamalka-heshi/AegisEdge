package detector

import (
	"context"
	"errors"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Detector domain errors.
var (
	ErrNilBatch            = errors.New("telemetry batch cannot be nil")
	ErrInvalidRuleConfig   = errors.New("invalid detector rule configuration")
	ErrDuplicateMetricRule = errors.New("duplicate metric rule configuration")
)

// Detector defines the contract for edge anomaly detectors.
// Implementations evaluate metric samples or batches against deterministic or statistical rules
// without network dependencies or downstream incident coupling.
type Detector interface {
	// Name returns the identifier of this detector implementation.
	Name() string

	// Version returns the version string of the detector algorithm or rule set.
	Version() string

	// Detect evaluates a single MetricSample against configured detection rules.
	// Returns an AnomalySignal if an anomaly is detected, or nil if normal / not evaluated.
	// Returns an error if the sample fails domain validation.
	Detect(ctx context.Context, sample types.MetricSample) (*types.AnomalySignal, error)

	// DetectBatch evaluates all metrics in a TelemetryBatch against configured detection rules.
	// Returns a slice of detected AnomalySignals (empty slice if none detected).
	DetectBatch(ctx context.Context, batch *types.TelemetryBatch) ([]*types.AnomalySignal, error)
}
