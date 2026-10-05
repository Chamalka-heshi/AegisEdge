package verification

import (
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
)

var (
	ErrInvalidRequiredObservations = errors.New("required consecutive observations must be between 1 and 100")
	ErrInvalidVerificationTimeout  = errors.New("verification timeout must be positive and not exceed max timeout")
)

// VerificationConfig defines operational parameters for the verification engine.
type VerificationConfig struct {
	// RequiredConsecutiveObservations is the number of consecutive healthy telemetry
	// samples required to confirm recovery. Defaults to 3.
	RequiredConsecutiveObservations int

	// VerificationTimeout is the observation window duration. Defaults to 5 minutes.
	VerificationTimeout time.Duration

	// MaxVerificationTimeout caps any custom timeout. Defaults to 24 hours.
	MaxVerificationTimeout time.Duration

	// Store optionally provides SQLite persistence for durable verification state.
	Store storage.VerificationStore

	// Clock provides time abstraction. Defaults to RealClock.
	Clock Clock

	// IDGenerator optionally overrides UUID generation for deterministic tests.
	IDGenerator func() (string, error)
}

// DefaultConfig returns recommended verification configuration defaults:
// 3 consecutive healthy observations within 5 minutes.
func DefaultConfig() VerificationConfig {
	return VerificationConfig{
		RequiredConsecutiveObservations: 3,
		VerificationTimeout:             5 * time.Minute,
		MaxVerificationTimeout:          24 * time.Hour,
		Clock:                           RealClock{},
		IDGenerator:                     defaultUUIDGenerator,
	}
}

// Validate verifies the internal bounds of VerificationConfig.
func (c *VerificationConfig) Validate() error {
	if c.RequiredConsecutiveObservations <= 0 || c.RequiredConsecutiveObservations > 100 {
		return ErrInvalidRequiredObservations
	}
	if c.VerificationTimeout <= 0 {
		return ErrInvalidVerificationTimeout
	}
	if c.MaxVerificationTimeout <= 0 {
		c.MaxVerificationTimeout = 24 * time.Hour
	}
	if c.VerificationTimeout > c.MaxVerificationTimeout {
		return ErrInvalidVerificationTimeout
	}
	if c.Clock == nil {
		c.Clock = RealClock{}
	}
	if c.IDGenerator == nil {
		c.IDGenerator = defaultUUIDGenerator
	}
	return nil
}

func defaultUUIDGenerator() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("ver-%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
