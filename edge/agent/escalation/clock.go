package escalation

import (
	"sync"
	"time"
)

// Clock abstracts time measurement for deterministic testing of cooldowns and failure windows.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
}

// RealClock uses the standard system clock in UTC.
type RealClock struct{}

// Now returns the current system time in UTC.
func (RealClock) Now() time.Time {
	return time.Now().UTC()
}

// Since returns the time elapsed since t in UTC.
func (RealClock) Since(t time.Time) time.Duration {
	return time.Now().UTC().Sub(t)
}

// FakeClock provides a thread-safe synthetic clock for deterministic testing.
type FakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewFakeClock initializes a FakeClock at the given start time in UTC.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{
		now: start.UTC(),
	}
}

// Now returns the synthetic current time in UTC.
func (f *FakeClock) Now() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.now
}

// Since returns the synthetic time elapsed since t.
func (f *FakeClock) Since(t time.Time) time.Duration {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.now.Sub(t)
}

// Set explicitly sets the synthetic time.
func (f *FakeClock) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t.UTC()
}

// Advance moves the synthetic clock forward by duration d.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}
