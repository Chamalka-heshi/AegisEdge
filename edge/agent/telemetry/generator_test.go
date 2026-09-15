package telemetry

import (
	"context"
	"testing"
	"time"
)

func TestSimulatedGenerator_MonotonicSequence(t *testing.T) {
	ctx := context.Background()
	const startSeq = int64(10)
	nodeID := "node-test-monotonic"

	gen, err := NewSimulatedGenerator(nodeID, startSeq)
	if err != nil {
		t.Fatalf("NewSimulatedGenerator failed: %v", err)
	}

	seenUUIDs := make(map[string]bool)

	for i := int64(0); i < 5; i++ {
		batch, err := gen.GenerateBatch(ctx)
		if err != nil {
			t.Fatalf("GenerateBatch failed on step %d: %v", i, err)
		}

		expectedSeq := startSeq + i
		if batch.SequenceNumber != expectedSeq {
			t.Errorf("step %d sequence mismatch: got %d, want %d", i, batch.SequenceNumber, expectedSeq)
		}

		if batch.NodeID != nodeID {
			t.Errorf("nodeID mismatch: got %s, want %s", batch.NodeID, nodeID)
		}

		if seenUUIDs[batch.BatchID] {
			t.Fatalf("duplicate UUID generated: %s", batch.BatchID)
		}
		seenUUIDs[batch.BatchID] = true

		if len(batch.Metrics) != 4 {
			t.Errorf("expected 4 metrics, got %d", len(batch.Metrics))
		}

		// Ensure all metric samples carry the correct nodeID and valid finite values
		for _, m := range batch.Metrics {
			if m.NodeID != nodeID {
				t.Errorf("metric sample NodeID %s != batch NodeID %s", m.NodeID, nodeID)
			}
			if m.Value < 0 || m.Value > 100 {
				t.Errorf("metric %s value %f out of expected range [0, 100]", m.Name, m.Value)
			}
		}
	}
}

func TestSimulatedGenerator_EmptyNodeID(t *testing.T) {
	_, err := NewSimulatedGenerator("", 0)
	if err == nil {
		t.Fatal("expected error for empty nodeID, got nil")
	}

	_, err = NewSimulatedGenerator("   ", 0)
	if err == nil {
		t.Fatal("expected error for whitespace nodeID, got nil")
	}
}

func TestSimulatedGenerator_DeterministicValues(t *testing.T) {
	ctx := context.Background()
	fixedTime := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

	gen1, _ := NewSimulatedGenerator("node-1", 0)
	gen1.SetTimeProvider(func() time.Time { return fixedTime })

	gen2, _ := NewSimulatedGenerator("node-1", 0)
	gen2.SetTimeProvider(func() time.Time { return fixedTime })

	b1, err := gen1.GenerateBatch(ctx)
	if err != nil {
		t.Fatalf("gen1 failed: %v", err)
	}

	b2, err := gen2.GenerateBatch(ctx)
	if err != nil {
		t.Fatalf("gen2 failed: %v", err)
	}

	if b1.SequenceNumber != b2.SequenceNumber {
		t.Errorf("sequence mismatch: %d vs %d", b1.SequenceNumber, b2.SequenceNumber)
	}

	for i := range b1.Metrics {
		if b1.Metrics[i].Name != b2.Metrics[i].Name {
			t.Errorf("metric name mismatch: %s vs %s", b1.Metrics[i].Name, b2.Metrics[i].Name)
		}
		if b1.Metrics[i].Value != b2.Metrics[i].Value {
			t.Errorf("metric value mismatch: %f vs %f", b1.Metrics[i].Value, b2.Metrics[i].Value)
		}
	}
}
