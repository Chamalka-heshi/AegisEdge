package telemetry

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Generator defines the contract for producing validated, sequence-numbered telemetry batches.
type Generator interface {
	// GenerateBatch generates the next deterministic TelemetryBatch.
	GenerateBatch(ctx context.Context) (*types.TelemetryBatch, error)

	// CurrentSequence returns the last assigned sequence number.
	CurrentSequence() int64
}

// SimulatedGenerator produces deterministic, bounded synthetic metrics.
type SimulatedGenerator struct {
	nodeID       string
	sequence     atomic.Int64
	step         int64
	mu           sync.Mutex
	timeProvider func() time.Time
}

// NewSimulatedGenerator initializes the generator.
// initialSequence should be set to (latestPersistedSequence + 1).
func NewSimulatedGenerator(nodeID string, initialSequence int64) (*SimulatedGenerator, error) {
	if strings.TrimSpace(nodeID) == "" {
		return nil, errors.New("nodeID cannot be empty")
	}
	if initialSequence < 0 {
		initialSequence = 0
	}

	gen := &SimulatedGenerator{
		nodeID:       nodeID,
		timeProvider: func() time.Time { return time.Now().UTC() },
	}
	gen.sequence.Store(initialSequence)
	return gen, nil
}

// SetTimeProvider overrides wall-clock time for deterministic testing.
func (g *SimulatedGenerator) SetTimeProvider(tp func() time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.timeProvider = tp
}

// GenerateBatch generates a new sequence-numbered batch with deterministic synthetic metrics.
func (g *SimulatedGenerator) GenerateBatch(ctx context.Context) (*types.TelemetryBatch, error) {
	g.mu.Lock()
	step := g.step
	g.step++
	now := g.timeProvider()
	g.mu.Unlock()

	// Assign monotonic sequence number atomically
	seq := g.sequence.Add(1) - 1

	batchID, err := newUUIDv4()
	if err != nil {
		return nil, fmt.Errorf("failed to generate stable batch UUID: %w", err)
	}

	// Produce deterministic, realistic values bounded within safe operating ranges
	cpu := math.Round((40.0+20.0*math.Sin(float64(step)*0.2))*10) / 10
	mem := math.Round((60.0+10.0*math.Cos(float64(step)*0.1))*10) / 10
	disk := math.Round((35.0+float64(step%50)*0.1)*10) / 10
	temp := math.Round((50.0+8.0*math.Sin(float64(step)*0.25))*10) / 10

	metrics := []types.MetricSample{
		{
			NodeID:    g.nodeID,
			Name:      "cpu_usage_percent",
			Value:     cpu,
			Unit:      "percent",
			Timestamp: now,
		},
		{
			NodeID:    g.nodeID,
			Name:      "memory_usage_percent",
			Value:     mem,
			Unit:      "percent",
			Timestamp: now,
		},
		{
			NodeID:    g.nodeID,
			Name:      "disk_usage_percent",
			Value:     disk,
			Unit:      "percent",
			Timestamp: now,
		},
		{
			NodeID:    g.nodeID,
			Name:      "temperature_celsius",
			Value:     temp,
			Unit:      "celsius",
			Timestamp: now,
		},
	}

	batch := &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         g.nodeID,
		SequenceNumber: seq,
		CollectedAt:    now,
		Metrics:        metrics,
	}

	// Ensure the batch strictly satisfies domain contracts before yielding
	if err := batch.Validate(); err != nil {
		return nil, fmt.Errorf("generated batch failed domain validation: %w", err)
	}

	return batch, nil
}

// CurrentSequence returns the last assigned sequence number.
func (g *SimulatedGenerator) CurrentSequence() int64 {
	return g.sequence.Load() - 1
}

// newUUIDv4 generates an RFC 4122 compliant UUIDv4 using standard crypto/rand.
func newUUIDv4() (string, error) {
	var b [16]byte
	_, err := rand.Read(b[:])
	if err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // Variant 10xx

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:16],
	), nil
}
