package audit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
)

// Clock abstracts system time to enable deterministic verification.
type Clock interface {
	Now() time.Time
}

// RealClock returns the system UTC time.
type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now().UTC()
}

// FakeClock provides synthetic, thread-safe time progression for deterministic unit tests.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock constructs a FakeClock initialized to the given time.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t.UTC()}
}

// Now returns synthetic current time.
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance advances synthetic time by d.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Recorder defines the contract for recording structured incident-response audit events.
type Recorder interface {
	// Record validates and durably records an audit event in local storage.
	// Returns ErrDuplicateEvent if the deterministic EventID is already recorded.
	Record(ctx context.Context, event AuditEvent) error

	// Query returns the query service for retrieving audit history.
	Query() QueryService

	// Close flushes and shuts down the recorder.
	Close() error
}

// QueryService defines the query contract for explaining past incident response decisions.
type QueryService interface {
	// GetEvent retrieves a single audit event by its unique EventID.
	GetEvent(ctx context.Context, eventID string) (*AuditEvent, error)

	// ListByIncident retrieves audit events for a given incident_id, ordered chronologically.
	ListByIncident(ctx context.Context, incidentID string, limit int) ([]AuditEvent, error)

	// ListByCorrelation retrieves audit events bound to a specific correlation_id, ordered chronologically.
	ListByCorrelation(ctx context.Context, correlationID string, limit int) ([]AuditEvent, error)

	// ListByNode retrieves audit events recorded for a given node_id, ordered chronologically.
	ListByNode(ctx context.Context, nodeID string, limit int) ([]AuditEvent, error)

	// List retrieves audit events matching an arbitrary filter, bounded by limit.
	List(ctx context.Context, filter storage.AuditFilter) ([]AuditEvent, error)

	// Count returns the total number of recorded audit events.
	Count(ctx context.Context) (int64, error)
}

// Service implements both Recorder and QueryService backed by a durable storage.AuditStore.
type Service struct {
	mu     sync.RWMutex
	store  storage.AuditStore
	clock  Clock
	closed bool
}

// NewService constructs an audit Service wrapping the given storage.AuditStore and Clock.
func NewService(store storage.AuditStore, clock Clock) (*Service, error) {
	if store == nil {
		return nil, errors.New("audit store cannot be nil")
	}
	if clock == nil {
		clock = RealClock{}
	}
	return &Service{
		store: store,
		clock: clock,
	}, nil
}

// Record validates and durably persists an audit event.
func (s *Service) Record(ctx context.Context, event AuditEvent) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return storage.ErrStoreClosed
	}
	s.mu.RUnlock()

	// Default correlation ID if unset
	if event.CorrelationID == "" && event.IncidentID != "" {
		event.CorrelationID = ComputeCorrelationID(event.IncidentID)
	}

	// Default timestamp if unset
	if event.Timestamp.IsZero() {
		event.Timestamp = s.clock.Now()
	}

	// Default deterministic event ID if unset
	if event.EventID == "" {
		discriminator := event.ActionID
		if discriminator == "" {
			discriminator = event.DecisionID
		}
		if discriminator == "" {
			discriminator = event.Reason
		}
		event.EventID = ComputeEventID(event.NodeID, event.IncidentID, event.EventType, 0, discriminator)
	}

	// Strict domain validation
	if err := event.Validate(); err != nil {
		return err
	}

	stored := event.ToStored()
	if err := s.store.RecordAuditEvent(ctx, stored); err != nil {
		if errors.Is(err, storage.ErrDuplicateAuditEvent) {
			return ErrDuplicateEvent
		}
		return fmt.Errorf("failed to persist audit event: %w", err)
	}

	return nil
}

// Query returns self as the QueryService implementation.
func (s *Service) Query() QueryService {
	return s
}

// Close marks the service closed.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// GetEvent retrieves an audit event by its unique EventID.
func (s *Service) GetEvent(ctx context.Context, eventID string) (*AuditEvent, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, storage.ErrStoreClosed
	}
	s.mu.RUnlock()

	stored, err := s.store.GetAuditEvent(ctx, eventID)
	if errors.Is(err, storage.ErrAuditEventNotFound) {
		return nil, ErrEventNotFound
	}
	if err != nil {
		return nil, err
	}
	evt := AuditEventFromStored(stored)
	return &evt, nil
}

// ListByIncident retrieves audit events for a given incident_id, ordered chronologically.
func (s *Service) ListByIncident(ctx context.Context, incidentID string, limit int) ([]AuditEvent, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, storage.ErrStoreClosed
	}
	s.mu.RUnlock()

	storedEvents, err := s.store.ListAuditEventsByIncident(ctx, incidentID, limit)
	if err != nil {
		return nil, err
	}
	return toDomainEvents(storedEvents), nil
}

// ListByCorrelation retrieves audit events bound to a specific correlation_id, ordered chronologically.
func (s *Service) ListByCorrelation(ctx context.Context, correlationID string, limit int) ([]AuditEvent, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, storage.ErrStoreClosed
	}
	s.mu.RUnlock()

	storedEvents, err := s.store.ListAuditEventsByCorrelation(ctx, correlationID, limit)
	if err != nil {
		return nil, err
	}
	return toDomainEvents(storedEvents), nil
}

// ListByNode retrieves audit events recorded for a given node_id, ordered chronologically.
func (s *Service) ListByNode(ctx context.Context, nodeID string, limit int) ([]AuditEvent, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, storage.ErrStoreClosed
	}
	s.mu.RUnlock()

	storedEvents, err := s.store.ListAuditEventsByNode(ctx, nodeID, limit)
	if err != nil {
		return nil, err
	}
	return toDomainEvents(storedEvents), nil
}

// List retrieves audit events matching an arbitrary filter, bounded by limit.
func (s *Service) List(ctx context.Context, filter storage.AuditFilter) ([]AuditEvent, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, storage.ErrStoreClosed
	}
	s.mu.RUnlock()

	storedEvents, err := s.store.ListAuditEvents(ctx, filter)
	if err != nil {
		return nil, err
	}
	return toDomainEvents(storedEvents), nil
}

// Count returns the total number of recorded audit events.
func (s *Service) Count(ctx context.Context) (int64, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, storage.ErrStoreClosed
	}
	s.mu.RUnlock()

	return s.store.CountAuditEvents(ctx)
}

func toDomainEvents(stored []*storage.StoredAuditEvent) []AuditEvent {
	result := make([]AuditEvent, len(stored))
	for i, s := range stored {
		result[i] = AuditEventFromStored(s)
	}
	return result
}
