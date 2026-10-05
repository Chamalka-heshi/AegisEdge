package verification

import (
	"sync"
	"time"
)

// Clock provides an abstraction over wall-clock time for deterministic testing.
type Clock interface {
	Now() time.Time
}

// RealClock implements Clock using the system UTC clock.
type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now().UTC()
}

// FakeClock implements Clock with manual time advancement for deterministic tests.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock constructs a FakeClock initialized to the given time.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start.UTC()}
}

// Now returns the current fake clock time.
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance shifts the fake clock forward by duration d.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set sets the fake clock to an explicit time.
func (f *FakeClock) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t.UTC()
}
