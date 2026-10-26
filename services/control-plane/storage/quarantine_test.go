package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

func TestQuarantine_RecordAndGetByID(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	seq := uint64(101)
	payload := []byte(`{"event":"agent_boot","data":"diagnostic"}`)
	rec := &QuarantineRecord{
		SourceStream:    "AEGISEDGE_TELEMETRY",
		SourceSequence:  &seq,
		SourceSubject:   "aegisedge.telemetry.v1.node-1",
		NodeID:          "node-1",
		BatchID:         "batch-alpha",
		Payload:         payload,
		FailureCategory: CategoryDeliveryExhausted,
		FailureReason:   "storage busy lock timeout",
		DeliveryCount:   5,
		ReplayMetadata: map[string]any{
			"env": "production",
		},
	}

	saved, err := store.RecordQuarantine(ctx, rec)
	if err != nil {
		t.Fatalf("RecordQuarantine failed: %v", err)
	}

	if saved.ID == "" || !strings.HasPrefix(saved.ID, "qrn-") {
		t.Errorf("expected stable qrn- ID, got %q", saved.ID)
	}
	if saved.Attempts != 1 {
		t.Errorf("expected Attempts=1, got %d", saved.Attempts)
	}
	if saved.Status != StatusQuarantined {
		t.Errorf("expected Status=quarantined, got %s", saved.Status)
	}
	if saved.DeduplicationKey != "stream:AEGISEDGE_TELEMETRY:101" {
		t.Errorf("expected dedup_key 'stream:AEGISEDGE_TELEMETRY:101', got %q", saved.DeduplicationKey)
	}
	if saved.FirstQuarantinedAt.IsZero() || saved.LastQuarantinedAt.IsZero() {
		t.Errorf("expected non-zero quarantine timestamps")
	}
	if saved.LastFailureCategory != CategoryDeliveryExhausted {
		t.Errorf("expected LastFailureCategory=%s, got %s", CategoryDeliveryExhausted, saved.LastFailureCategory)
	}
	if saved.LastFailureReason != "storage busy lock timeout" {
		t.Errorf("expected LastFailureReason matches initial reason, got %q", saved.LastFailureReason)
	}

	// Retrieve by ID
	fetched, err := store.GetQuarantineRecord(ctx, saved.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord failed: %v", err)
	}

	if fetched.ID != saved.ID {
		t.Errorf("ID mismatch: got %q, want %q", fetched.ID, saved.ID)
	}
	if fetched.SourceSequence == nil || *fetched.SourceSequence != 101 {
		t.Errorf("SourceSequence mismatch: got %v, want 101", fetched.SourceSequence)
	}
	if fetched.NodeID != "node-1" || fetched.BatchID != "batch-alpha" {
		t.Errorf("NodeID/BatchID mismatch: got %q/%q", fetched.NodeID, fetched.BatchID)
	}
	if !bytes.Equal(fetched.Payload, payload) {
		t.Errorf("Payload mismatch: got %s, want %s", string(fetched.Payload), string(payload))
	}
	if fetched.FailureCategory != CategoryDeliveryExhausted {
		t.Errorf("FailureCategory mismatch: got %s", fetched.FailureCategory)
	}
	if fetched.FailureReason != "storage busy lock timeout" {
		t.Errorf("FailureReason mismatch: got %q", fetched.FailureReason)
	}
	if fetched.LastFailureCategory != CategoryDeliveryExhausted {
		t.Errorf("LastFailureCategory mismatch: got %s", fetched.LastFailureCategory)
	}
	if fetched.LastFailureReason != "storage busy lock timeout" {
		t.Errorf("LastFailureReason mismatch: got %q", fetched.LastFailureReason)
	}
	if fetched.DeliveryCount != 5 {
		t.Errorf("DeliveryCount mismatch: got %d", fetched.DeliveryCount)
	}
	if fetched.ReplayMetadata["env"] != "production" {
		t.Errorf("ReplayMetadata mismatch: got %v", fetched.ReplayMetadata)
	}
}

func TestQuarantine_PersistenceAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "quarantine_persist.db")

	store1, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite 1 failed: %v", err)
	}

	seq := uint64(55)
	rec := &QuarantineRecord{
		SourceStream:    "AEGISEDGE_TELEMETRY",
		SourceSequence:  &seq,
		SourceSubject:   "aegisedge.telemetry.v1.node-2",
		Payload:         []byte("invalid json payload {"),
		FailureCategory: CategoryDeserialization,
		FailureReason:   "unexpected EOF",
		DeliveryCount:   1,
	}

	saved, err := store1.RecordQuarantine(ctx, rec)
	if err != nil {
		t.Fatalf("RecordQuarantine failed: %v", err)
	}

	if err := store1.Close(); err != nil {
		t.Fatalf("store1.Close failed: %v", err)
	}

	// Reopen database
	store2, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite 2 failed: %v", err)
	}
	defer store2.Close()

	fetched, err := store2.GetQuarantineRecord(ctx, saved.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord after reopen failed: %v", err)
	}

	if fetched.ID != saved.ID {
		t.Errorf("ID mismatch after reopen: got %q, want %q", fetched.ID, saved.ID)
	}
	if string(fetched.Payload) != "invalid json payload {" {
		t.Errorf("Payload mismatch after reopen: got %s", string(fetched.Payload))
	}
	if fetched.SourceSequence == nil || *fetched.SourceSequence != 55 {
		t.Errorf("SourceSequence mismatch after reopen: got %v", fetched.SourceSequence)
	}
	if fetched.FailureCategory != CategoryDeserialization {
		t.Errorf("FailureCategory mismatch after reopen: got %s", fetched.FailureCategory)
	}
	if fetched.LastFailureCategory != CategoryDeserialization {
		t.Errorf("LastFailureCategory mismatch after reopen: got %s", fetched.LastFailureCategory)
	}
}

func TestQuarantine_DeriveDeduplicationKey_Hierarchy(t *testing.T) {
	seq := uint64(77)
	payload := []byte(`{"test":"data"}`)

	// Priority 1: Stream + Seq
	k1 := DeriveDeduplicationKey("AEGISEDGE_TELEMETRY", &seq, "node-1", "batch-1", "subj", payload)
	if k1 != "stream:AEGISEDGE_TELEMETRY:77" {
		t.Errorf("expected stream dedup key, got %q", k1)
	}

	// Priority 2: NodeID + BatchID when stream is absent or seq is nil (length-prefixed encoding)
	k2 := DeriveDeduplicationKey("", nil, "node-1", "batch-1", "subj", payload)
	if k2 != "batch:6:node-1:7:batch-1" {
		t.Errorf("expected unambiguous length-prefixed batch dedup key, got %q", k2)
	}

	// Priority 3: Fallback content hash when neither stream nor node/batch is available
	k3 := DeriveDeduplicationKey("", nil, "", "", "aegisedge.telemetry.v1.unknown", payload)
	if !strings.HasPrefix(k3, "payload:sha256:") {
		t.Errorf("expected payload hash fallback key, got %q", k3)
	}

	// Different payload produces different hash key
	k4 := DeriveDeduplicationKey("", nil, "", "", "aegisedge.telemetry.v1.unknown", []byte(`{"different":"data"}`))
	if k3 == k4 {
		t.Errorf("expected different hash keys for different payloads, got %q == %q", k3, k4)
	}
}

func TestQuarantine_DuplicateIdentityHandling(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	seq := uint64(10)
	payload := []byte("identical payload across retries")
	rec1 := &QuarantineRecord{
		SourceStream:    "AEGISEDGE_TELEMETRY",
		SourceSequence:  &seq,
		SourceSubject:   "aegisedge.telemetry.v1.node-1",
		Payload:         payload,
		FailureCategory: CategorySchemaValidation,
		FailureReason:   "schema field invalid",
		DeliveryCount:   1,
	}

	saved1, err := store.RecordQuarantine(ctx, rec1)
	if err != nil {
		t.Fatalf("first RecordQuarantine failed: %v", err)
	}
	if saved1.Attempts != 1 {
		t.Errorf("expected Attempts=1, got %d", saved1.Attempts)
	}

	// Second quarantine submission for the exact same message with higher delivery count & new reason
	rec2 := &QuarantineRecord{
		SourceStream:    "AEGISEDGE_TELEMETRY",
		SourceSequence:  &seq,
		SourceSubject:   "aegisedge.telemetry.v1.node-1",
		Payload:         payload,
		FailureCategory: CategoryDeliveryExhausted,
		FailureReason:   "db lock attempt 5 exhausted",
		DeliveryCount:   5,
	}

	saved2, err := store.RecordQuarantine(ctx, rec2)
	if err != nil {
		t.Fatalf("second RecordQuarantine failed: %v", err)
	}

	// ID must match existing record
	if saved2.ID != saved1.ID {
		t.Errorf("expected same ID on duplicate submission: got %q, want %q", saved2.ID, saved1.ID)
	}
	if saved2.Attempts != 2 {
		t.Errorf("expected Attempts=2, got %d", saved2.Attempts)
	}
	if saved2.DeliveryCount != 5 {
		t.Errorf("expected DeliveryCount=5, got %d", saved2.DeliveryCount)
	}
	// Initial failure category and reason must be IMMUTABLE
	if saved2.FailureCategory != CategorySchemaValidation {
		t.Errorf("expected initial FailureCategory to remain %s, got %s", CategorySchemaValidation, saved2.FailureCategory)
	}
	if saved2.FailureReason != "schema field invalid" {
		t.Errorf("expected initial FailureReason to remain immutable, got %q", saved2.FailureReason)
	}
	// Latest failure diagnostics must reflect subsequent attempt
	if saved2.LastFailureCategory != CategoryDeliveryExhausted {
		t.Errorf("expected LastFailureCategory to be %s, got %s", CategoryDeliveryExhausted, saved2.LastFailureCategory)
	}
	if saved2.LastFailureReason != "db lock attempt 5 exhausted" {
		t.Errorf("expected LastFailureReason to be updated, got %q", saved2.LastFailureReason)
	}

	// Verify only 1 record exists in store
	list, err := store.ListQuarantineRecords(ctx, QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 record in quarantine, got %d", len(list))
	}
}

func TestQuarantine_PayloadMismatchConflict(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	seq := uint64(42)
	initialPayload := []byte("original payload bytes")
	rec1 := &QuarantineRecord{
		SourceStream:    "AEGISEDGE_TELEMETRY",
		SourceSequence:  &seq,
		Payload:         initialPayload,
		FailureCategory: CategoryDeliveryExhausted,
		FailureReason:   "initial fail",
	}

	saved, err := store.RecordQuarantine(ctx, rec1)
	if err != nil {
		t.Fatalf("initial RecordQuarantine failed: %v", err)
	}

	// 1. Same key, identical payload: idempotent duplicate success
	identicalRec := &QuarantineRecord{
		SourceStream:    "AEGISEDGE_TELEMETRY",
		SourceSequence:  &seq,
		Payload:         initialPayload,
		FailureCategory: CategoryDeliveryExhausted,
		FailureReason:   "second fail",
	}
	dup, err := store.RecordQuarantine(ctx, identicalRec)
	if err != nil {
		t.Fatalf("identical payload duplicate failed: %v", err)
	}
	if dup.Attempts != 2 {
		t.Errorf("expected Attempts=2, got %d", dup.Attempts)
	}

	// 2. Same key, different payload: explicit conflict error
	conflictingRec := &QuarantineRecord{
		SourceStream:    "AEGISEDGE_TELEMETRY",
		SourceSequence:  &seq,
		Payload:         []byte("tampered / conflicting payload bytes"),
		FailureCategory: CategoryDeliveryExhausted,
		FailureReason:   "third fail with different payload",
	}
	_, err = store.RecordQuarantine(ctx, conflictingRec)
	if err == nil {
		t.Fatalf("expected PayloadConflictError, got nil")
	}
	if !errors.Is(err, ErrPayloadMismatch) {
		t.Errorf("expected errors.Is(err, ErrPayloadMismatch), got %v", err)
	}
	var conflictErr *PayloadConflictError
	if !errors.As(err, &conflictErr) {
		t.Errorf("expected errors.As to match *PayloadConflictError, got %T", err)
	}

	// 3. Database contents remain untouched by the conflicting submission
	fetched, err := store.GetQuarantineRecord(ctx, saved.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord failed: %v", err)
	}
	if !bytes.Equal(fetched.Payload, initialPayload) {
		t.Errorf("persisted payload was altered! got %s, want %s", string(fetched.Payload), string(initialPayload))
	}
	if fetched.Attempts != 2 {
		t.Errorf("attempts should not increment on conflict: got %d, want 2", fetched.Attempts)
	}

	// 4. Same key after terminal status: terminal status is preserved
	_, err = store.UpdateQuarantineStatus(ctx, QuarantineStatusUpdate{
		ID:        saved.ID,
		NewStatus: StatusReplayed,
		Reason:    "replayed",
	})
	if err != nil {
		t.Fatalf("UpdateQuarantineStatus failed: %v", err)
	}

	afterTerminal, err := store.RecordQuarantine(ctx, identicalRec)
	if err != nil {
		t.Fatalf("RecordQuarantine after terminal failed: %v", err)
	}
	if afterTerminal.Status != StatusReplayed {
		t.Errorf("expected terminal status replayed, got %s", afterTerminal.Status)
	}
}

func TestQuarantine_UnambiguousBatchDeduplicationKey(t *testing.T) {
	// The collision identified in audit:
	// A: nodeID="edge:prod", batchID="batch-1"
	// B: nodeID="edge", batchID="prod:batch-1"
	keyA := DeriveDeduplicationKey("", nil, "edge:prod", "batch-1", "subj", []byte("data"))
	keyB := DeriveDeduplicationKey("", nil, "edge", "prod:batch-1", "subj", []byte("data"))

	if keyA == keyB {
		t.Fatalf("delimiter collision detected! keyA=%q, keyB=%q", keyA, keyB)
	}
	if keyA != "batch:9:edge:prod:7:batch-1" {
		t.Errorf("unexpected keyA format: %q", keyA)
	}
	if keyB != "batch:4:edge:12:prod:batch-1" {
		t.Errorf("unexpected keyB format: %q", keyB)
	}

	// Empty and whitespace-only node or batch falls back to payload hash
	keyEmptyNode := DeriveDeduplicationKey("", nil, "   ", "batch-1", "subj", []byte("data"))
	if !strings.HasPrefix(keyEmptyNode, "payload:sha256:") {
		t.Errorf("expected payload hash fallback for empty node, got %q", keyEmptyNode)
	}
	keyEmptyBatch := DeriveDeduplicationKey("", nil, "node-1", "   ", "subj", []byte("data"))
	if !strings.HasPrefix(keyEmptyBatch, "payload:sha256:") {
		t.Errorf("expected payload hash fallback for empty batch, got %q", keyEmptyBatch)
	}

	// Unicode identifiers
	keyUnicode := DeriveDeduplicationKey("", nil, "ノード-1", "バッチ-99", "subj", []byte("data"))
	expectedLenNode := len("ノード-1")   // 11 bytes in UTF-8
	expectedLenBatch := len("バッチ-99") // 12 bytes in UTF-8
	expectedKey := fmt.Sprintf("batch:%d:ノード-1:%d:バッチ-99", expectedLenNode, expectedLenBatch)
	if keyUnicode != expectedKey {
		t.Errorf("expected unicode key %q, got %q", expectedKey, keyUnicode)
	}

	// Deterministic output
	keyAgain := DeriveDeduplicationKey("", nil, "node-1", "batch-1", "subj", []byte("data"))
	keySecond := DeriveDeduplicationKey("", nil, "node-1", "batch-1", "subj", []byte("data"))
	if keyAgain != keySecond {
		t.Errorf("expected deterministic derivation: %q != %q", keyAgain, keySecond)
	}
}

func TestQuarantine_SourceSequenceBounds(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// 1. math.MaxInt64 accepted
	maxSeq := uint64(math.MaxInt64)
	recValid := &QuarantineRecord{
		SourceStream:    "AEGISEDGE_TELEMETRY",
		SourceSequence:  &maxSeq,
		Payload:         []byte("data"),
		FailureCategory: CategoryDeliveryExhausted,
	}
	saved, err := store.RecordQuarantine(ctx, recValid)
	if err != nil {
		t.Fatalf("expected math.MaxInt64 to be accepted, got error: %v", err)
	}
	if saved.SourceSequence == nil || *saved.SourceSequence != maxSeq {
		t.Errorf("retrieved sequence mismatch: got %v, want %d", saved.SourceSequence, maxSeq)
	}

	// 2. math.MaxInt64 + 1 rejected
	overflowSeq := uint64(math.MaxInt64) + 1
	recOverflow := &QuarantineRecord{
		SourceStream:    "AEGISEDGE_TELEMETRY",
		SourceSequence:  &overflowSeq,
		Payload:         []byte("data"),
		FailureCategory: CategoryDeliveryExhausted,
	}
	_, err = store.RecordQuarantine(ctx, recOverflow)
	if err == nil {
		t.Fatalf("expected ErrSourceSequenceOverflow for sequence math.MaxInt64 + 1, got nil")
	}
	if !errors.Is(err, ErrSourceSequenceOverflow) {
		t.Errorf("expected errors.Is(err, ErrSourceSequenceOverflow), got %v", err)
	}

	// 3. nil source sequence remains valid
	recNilSeq := &QuarantineRecord{
		Payload:         []byte("nil sequence data"),
		FailureCategory: CategoryDeliveryExhausted,
	}
	savedNil, err := store.RecordQuarantine(ctx, recNilSeq)
	if err != nil {
		t.Fatalf("expected nil sequence to be valid, got: %v", err)
	}
	if savedNil.SourceSequence != nil {
		t.Errorf("expected nil SourceSequence, got %v", savedNil.SourceSequence)
	}
}

func TestQuarantine_SanitizeFailureReason_UTF8Boundaries(t *testing.T) {
	// 1. ASCII input below limit
	short := "simple error"
	sShort := SanitizeFailureReason(short)
	if sShort != short || !utf8.ValidString(sShort) {
		t.Errorf("failed on short ASCII: %q", sShort)
	}

	// 2. ASCII input exactly at limit
	exact := strings.Repeat("a", MaxFailureReasonLength)
	sExact := SanitizeFailureReason(exact)
	if len(sExact) != MaxFailureReasonLength || !utf8.ValidString(sExact) {
		t.Errorf("failed on exact limit ASCII: len=%d", len(sExact))
	}

	// 3. ASCII input above limit
	over := strings.Repeat("b", MaxFailureReasonLength+50)
	sOver := SanitizeFailureReason(over)
	if len(sOver) > MaxFailureReasonLength || !strings.HasSuffix(sOver, "...") || !utf8.ValidString(sOver) {
		t.Errorf("failed on over-limit ASCII: len=%d, valid=%v", len(sOver), utf8.ValidString(sOver))
	}

	// 4. Multibyte UTF-8 (Japanese 3-byte characters) crossing truncation boundary
	// MaxFailureReasonLength = 2048.
	// Fill 2044 bytes of 'a', then append a 3-byte Japanese character '日' (0xE6 0x97 0xA5)
	// 'a'*2044 + '日' = 2047 bytes. Append another '日' (2050 bytes, exceeds 2048).
	// Truncation target is 2048 - 3 = 2045.
	// 2045 falls right inside the first '日' (at byte 2 of 3).
	// A naive slice cleaned[:2045] would split '日' and produce invalid UTF-8!
	var mb strings.Builder
	mb.WriteString(strings.Repeat("a", 2044))
	mb.WriteString("日本東京")
	sMB := SanitizeFailureReason(mb.String())

	if !utf8.ValidString(sMB) {
		t.Fatalf("SanitizeFailureReason produced INVALID UTF-8 on multibyte boundary!")
	}
	if len(sMB) > MaxFailureReasonLength {
		t.Errorf("SanitizeFailureReason exceeded limit: len=%d (max %d)", len(sMB), MaxFailureReasonLength)
	}
	if !strings.HasSuffix(sMB, "...") {
		t.Errorf("expected ellipsis suffix, got %q", sMB[len(sMB)-10:])
	}

	// 5. 4-byte UTF-8 Emoji crossing boundary
	// 'a'*2043 + 4-byte emoji 🚀 (0xF0 0x9F 0x9A 0x80) + 🚀
	var emojiBuilder strings.Builder
	emojiBuilder.WriteString(strings.Repeat("x", 2043))
	emojiBuilder.WriteString("🚀🚀🚀")
	sEmoji := SanitizeFailureReason(emojiBuilder.String())

	if !utf8.ValidString(sEmoji) {
		t.Fatalf("SanitizeFailureReason produced INVALID UTF-8 on emoji boundary!")
	}
	if len(sEmoji) > MaxFailureReasonLength {
		t.Errorf("SanitizeFailureReason exceeded limit: len=%d", len(sEmoji))
	}
}

func TestQuarantine_TerminalStatusPreservedOnDuplicateInsert(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	seq := uint64(20)
	payload := []byte("payload to replay")
	rec := &QuarantineRecord{
		SourceStream:    "AEGISEDGE_TELEMETRY",
		SourceSequence:  &seq,
		Payload:         payload,
		FailureCategory: CategoryDeliveryExhausted,
		FailureReason:   "transient fail",
		DeliveryCount:   5,
	}

	saved, err := store.RecordQuarantine(ctx, rec)
	if err != nil {
		t.Fatalf("RecordQuarantine failed: %v", err)
	}

	// Operator marks as replayed
	updated, err := store.UpdateQuarantineStatus(ctx, QuarantineStatusUpdate{
		ID:        saved.ID,
		NewStatus: StatusReplayed,
		Reason:    "replayed successfully after db recovery",
		UpdatedBy: "operator",
	})
	if err != nil {
		t.Fatalf("UpdateQuarantineStatus failed: %v", err)
	}
	if updated.Status != StatusReplayed {
		t.Fatalf("expected status=replayed, got %s", updated.Status)
	}

	// Upstream redelivers same message and caller attempts to quarantine it again
	dup, err := store.RecordQuarantine(ctx, rec)
	if err != nil {
		t.Fatalf("subsequent RecordQuarantine failed: %v", err)
	}

	// Status must still be replayed, NOT reverted to quarantined!
	if dup.Status != StatusReplayed {
		t.Errorf("expected terminal status 'replayed' to be preserved, got %s", dup.Status)
	}
	if dup.Attempts != 2 {
		t.Errorf("expected Attempts=2, got %d", dup.Attempts)
	}

	// Confirm in database
	fetched, err := store.GetQuarantineRecord(ctx, saved.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord failed: %v", err)
	}
	if fetched.Status != StatusReplayed {
		t.Errorf("expected status in db to be replayed, got %s", fetched.Status)
	}
}

func TestQuarantine_ValidationAndBounds(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// 1. Nil record
	if _, err := store.RecordQuarantine(ctx, nil); !errors.Is(err, ErrInvalidQuarantineRecord) {
		t.Errorf("expected ErrInvalidQuarantineRecord for nil, got %v", err)
	}

	// 2. Oversized payload (> 1 MB)
	oversizedPayload := make([]byte, MaxQuarantinePayloadSize+100)
	recOversized := &QuarantineRecord{
		Payload:         oversizedPayload,
		FailureCategory: CategoryDeliveryExhausted,
	}
	if _, err := store.RecordQuarantine(ctx, recOversized); !errors.Is(err, ErrPayloadTooLarge) {
		t.Errorf("expected ErrPayloadTooLarge, got %v", err)
	}

	// 3. Oversized failure reason (> 2048 chars)
	recLongReason := &QuarantineRecord{
		Payload:         []byte("data"),
		FailureCategory: CategoryDeliveryExhausted,
		FailureReason:   strings.Repeat("e", MaxFailureReasonLength+10),
	}
	if _, err := store.RecordQuarantine(ctx, recLongReason); !errors.Is(err, ErrFailureReasonTooLong) {
		t.Errorf("expected ErrFailureReasonTooLong, got %v", err)
	}

	// 4. Negative delivery count
	recNegDeliv := &QuarantineRecord{
		Payload:       []byte("data"),
		DeliveryCount: -1,
	}
	if _, err := store.RecordQuarantine(ctx, recNegDeliv); !errors.Is(err, ErrInvalidQuarantineRecord) {
		t.Errorf("expected ErrInvalidQuarantineRecord for negative delivery count, got %v", err)
	}

	// 5. Invalid failure category
	recInvalidCat := &QuarantineRecord{
		Payload:         []byte("data"),
		FailureCategory: "not_a_category",
	}
	if _, err := store.RecordQuarantine(ctx, recInvalidCat); !errors.Is(err, ErrInvalidQuarantineRecord) {
		t.Errorf("expected ErrInvalidQuarantineRecord for invalid category, got %v", err)
	}

	// 6. Invalid status
	recInvalidStatus := &QuarantineRecord{
		Payload: []byte("data"),
		Status:  "bogus_status",
	}
	if _, err := store.RecordQuarantine(ctx, recInvalidStatus); !errors.Is(err, ErrInvalidQuarantineRecord) {
		t.Errorf("expected ErrInvalidQuarantineRecord for invalid status, got %v", err)
	}
}

func TestQuarantine_SanitizeFailureReason(t *testing.T) {
	raw := "Error connecting to db: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.secret token with password=supersecretpass and \x00null\x01byte"
	cleaned := SanitizeFailureReason(raw)

	if strings.Contains(cleaned, "\x00") || strings.Contains(cleaned, "\x01") {
		t.Errorf("expected null/control bytes stripped, got %q", cleaned)
	}
	if strings.Contains(cleaned, "eyJhbGci") {
		t.Errorf("expected bearer token redacted, got %q", cleaned)
	}
	if strings.Contains(cleaned, "supersecretpass") {
		t.Errorf("expected password redacted, got %q", cleaned)
	}
	if !strings.Contains(cleaned, "[REDACTED]") {
		t.Errorf("expected [REDACTED] placeholder in cleaned reason, got %q", cleaned)
	}
}

func TestQuarantine_ListAndPagination(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// Insert 10 records with explicit sequences and node IDs
	for i := 1; i <= 10; i++ {
		seq := uint64(i)
		nodeID := "node-1"
		if i > 5 {
			nodeID = "node-2"
		}
		_, err := store.RecordQuarantine(ctx, &QuarantineRecord{
			SourceStream:   "AEGISEDGE_TELEMETRY",
			SourceSequence: &seq,
			NodeID:         nodeID,
			Payload:        fmt.Appendf(nil, "batch %d", i),
			DeliveryCount:  i,
		})
		if err != nil {
			t.Fatalf("RecordQuarantine %d failed: %v", i, err)
		}
	}

	// Page 1: limit 4, offset 0
	p1, err := store.ListQuarantineRecords(ctx, QuarantineFilter{Limit: 4, Offset: 0})
	if err != nil {
		t.Fatalf("List page 1 failed: %v", err)
	}
	if len(p1) != 4 {
		t.Errorf("expected 4 records on page 1, got %d", len(p1))
	}

	// Page 2: limit 4, offset 4
	p2, err := store.ListQuarantineRecords(ctx, QuarantineFilter{Limit: 4, Offset: 4})
	if err != nil {
		t.Fatalf("List page 2 failed: %v", err)
	}
	if len(p2) != 4 {
		t.Errorf("expected 4 records on page 2, got %d", len(p2))
	}

	// Page 3: limit 4, offset 8 (remaining 2)
	p3, err := store.ListQuarantineRecords(ctx, QuarantineFilter{Limit: 4, Offset: 8})
	if err != nil {
		t.Fatalf("List page 3 failed: %v", err)
	}
	if len(p3) != 2 {
		t.Errorf("expected 2 records on page 3, got %d", len(p3))
	}

	// Filter by node_id = "node-2"
	byNode, err := store.ListQuarantineRecords(ctx, QuarantineFilter{NodeID: "node-2"})
	if err != nil {
		t.Fatalf("List by node failed: %v", err)
	}
	if len(byNode) != 5 {
		t.Errorf("expected 5 records for node-2, got %d", len(byNode))
	}

	// Filter by status = StatusQuarantined
	byStatus, err := store.ListQuarantineRecords(ctx, QuarantineFilter{Status: StatusQuarantined})
	if err != nil {
		t.Fatalf("List by status failed: %v", err)
	}
	if len(byStatus) != 10 {
		t.Errorf("expected 10 records with status quarantined, got %d", len(byStatus))
	}

	// Filter by non-existent status (e.g. replayed) -> empty list
	byReplayed, err := store.ListQuarantineRecords(ctx, QuarantineFilter{Status: StatusReplayed})
	if err != nil {
		t.Fatalf("List by replayed failed: %v", err)
	}
	if len(byReplayed) != 0 || byReplayed == nil {
		t.Errorf("expected empty non-nil slice, got %v", byReplayed)
	}

	// Invalid status in filter -> error
	_, err = store.ListQuarantineRecords(ctx, QuarantineFilter{Status: "bogus"})
	if !errors.Is(err, ErrInvalidQuarantineRecord) {
		t.Errorf("expected ErrInvalidQuarantineRecord for bogus filter status, got %v", err)
	}
}

func TestQuarantine_StatusTransitions(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	rec, err := store.RecordQuarantine(ctx, &QuarantineRecord{
		Payload:         []byte("data"),
		FailureCategory: CategoryDeliveryExhausted,
	})
	if err != nil {
		t.Fatalf("RecordQuarantine failed: %v", err)
	}

	// Transition 1: quarantined -> quarantined (same status) -> rejected
	_, err = store.UpdateQuarantineStatus(ctx, QuarantineStatusUpdate{
		ID:        rec.ID,
		NewStatus: StatusQuarantined,
	})
	if !errors.Is(err, ErrInvalidStatusTransition) {
		t.Errorf("expected ErrInvalidStatusTransition for no-op transition, got %v", err)
	}

	// Transition 2: ExpectedStatus mismatch -> ErrStatusConflict
	_, err = store.UpdateQuarantineStatus(ctx, QuarantineStatusUpdate{
		ID:             rec.ID,
		ExpectedStatus: StatusDiscarded, // Expected discarded, but currently quarantined
		NewStatus:      StatusReplayed,
	})
	if !errors.Is(err, ErrStatusConflict) {
		t.Errorf("expected ErrStatusConflict for ExpectedStatus mismatch, got %v", err)
	}

	// Transition 3: quarantined -> discarded (valid)
	discarded, err := store.UpdateQuarantineStatus(ctx, QuarantineStatusUpdate{
		ID:             rec.ID,
		ExpectedStatus: StatusQuarantined,
		NewStatus:      StatusDiscarded,
		Reason:         "obsolete node telemetry",
		UpdatedBy:      "operator-chamalka",
	})
	if err != nil {
		t.Fatalf("UpdateQuarantineStatus to discarded failed: %v", err)
	}
	if discarded.Status != StatusDiscarded {
		t.Errorf("expected status discarded, got %s", discarded.Status)
	}
	if discarded.StatusReason != "obsolete node telemetry" {
		t.Errorf("expected status reason 'obsolete node telemetry', got %q", discarded.StatusReason)
	}
	if discarded.StatusUpdatedBy != "operator-chamalka" {
		t.Errorf("expected updated by 'operator-chamalka', got %q", discarded.StatusUpdatedBy)
	}

	// Transition 4: discarded (terminal) -> replayed -> rejected
	_, err = store.UpdateQuarantineStatus(ctx, QuarantineStatusUpdate{
		ID:        rec.ID,
		NewStatus: StatusReplayed,
	})
	if !errors.Is(err, ErrInvalidStatusTransition) {
		t.Errorf("expected ErrInvalidStatusTransition from terminal discarded, got %v", err)
	}

	// Transition 5: discarded (terminal) -> quarantined -> rejected
	_, err = store.UpdateQuarantineStatus(ctx, QuarantineStatusUpdate{
		ID:        rec.ID,
		NewStatus: StatusQuarantined,
	})
	if !errors.Is(err, ErrInvalidStatusTransition) {
		t.Errorf("expected ErrInvalidStatusTransition from terminal discarded, got %v", err)
	}

	// Transition 6: non-existent ID -> ErrQuarantineNotFound
	_, err = store.UpdateQuarantineStatus(ctx, QuarantineStatusUpdate{
		ID:        "qrn-nonexistent-1234",
		NewStatus: StatusDiscarded,
	})
	if !errors.Is(err, ErrQuarantineNotFound) {
		t.Errorf("expected ErrQuarantineNotFound for non-existent record, got %v", err)
	}
}

func TestQuarantine_ConcurrentDuplicateInserts(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	seq := uint64(999)
	payload := []byte("consistent concurrent payload")
	concurrency := 20
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := store.RecordQuarantine(ctx, &QuarantineRecord{
				SourceStream:    "AEGISEDGE_TELEMETRY",
				SourceSequence:  &seq,
				Payload:         payload,
				FailureCategory: CategoryDeliveryExhausted,
				DeliveryCount:   idx,
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent RecordQuarantine returned error: %v", err)
	}

	list, err := store.ListQuarantineRecords(ctx, QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected exactly 1 quarantined record, got %d", len(list))
	}
	if list[0].Attempts != concurrency {
		t.Errorf("expected Attempts=%d, got %d", concurrency, list[0].Attempts)
	}
}

func TestQuarantine_ConcurrentStatusUpdates(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	rec, err := store.RecordQuarantine(ctx, &QuarantineRecord{
		Payload:         []byte("data"),
		FailureCategory: CategoryDeliveryExhausted,
	})
	if err != nil {
		t.Fatalf("RecordQuarantine failed: %v", err)
	}

	concurrency := 10
	var wg sync.WaitGroup
	successCount := 0
	conflictCount := 0
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := store.UpdateQuarantineStatus(ctx, QuarantineStatusUpdate{
				ID:             rec.ID,
				ExpectedStatus: StatusQuarantined,
				NewStatus:      StatusReplayed,
				Reason:         fmt.Sprintf("replayed by worker %d", idx),
			})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successCount++
			} else if errors.Is(err, ErrStatusConflict) || errors.Is(err, ErrInvalidStatusTransition) {
				conflictCount++
			} else {
				t.Errorf("unexpected error in concurrent status update: %v", err)
			}
		}(i)
	}

	wg.Wait()

	if successCount != 1 {
		t.Errorf("expected exactly 1 successful transition to replayed, got %d", successCount)
	}
	if conflictCount != concurrency-1 {
		t.Errorf("expected %d conflicts, got %d", concurrency-1, conflictCount)
	}

	fetched, err := store.GetQuarantineRecord(ctx, rec.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord failed: %v", err)
	}
	if fetched.Status != StatusReplayed {
		t.Errorf("expected final status 'replayed', got %s", fetched.Status)
	}
}

func TestQuarantine_TransactionRollbackOnFailure(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// Inject error after quarantine insert
	store.HookAfterQuarantineInsert = func() error {
		return errors.New("simulated disk I/O failure")
	}

	_, err = store.RecordQuarantine(ctx, &QuarantineRecord{
		Payload:         []byte("test"),
		FailureCategory: CategoryDeliveryExhausted,
	})
	if err == nil || !strings.Contains(err.Error(), "simulated disk I/O failure") {
		t.Fatalf("expected simulated error, got %v", err)
	}

	// Verify rollback: no records in table
	store.HookAfterQuarantineInsert = nil
	list, err := store.ListQuarantineRecords(ctx, QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("expected 0 records after rollback, got %d", len(list))
	}

	// Test HookBeforeQuarantineStatusCommit rollback
	saved, err := store.RecordQuarantine(ctx, &QuarantineRecord{
		Payload:         []byte("test 2"),
		FailureCategory: CategoryDeliveryExhausted,
	})
	if err != nil {
		t.Fatalf("RecordQuarantine failed: %v", err)
	}

	store.HookBeforeQuarantineStatusCommit = func() error {
		return errors.New("simulated status commit error")
	}

	_, err = store.UpdateQuarantineStatus(ctx, QuarantineStatusUpdate{
		ID:        saved.ID,
		NewStatus: StatusDiscarded,
	})
	if err == nil || !strings.Contains(err.Error(), "simulated status commit error") {
		t.Fatalf("expected simulated status error, got %v", err)
	}

	// Verify rollback: status must remain quarantined
	store.HookBeforeQuarantineStatusCommit = nil
	fetched, err := store.GetQuarantineRecord(ctx, saved.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord failed: %v", err)
	}
	if fetched.Status != StatusQuarantined {
		t.Errorf("expected status 'quarantined' after rollback, got %s", fetched.Status)
	}
}

func TestQuarantine_ClosedStoreRejection(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("store.Close failed: %v", err)
	}

	if _, err := store.RecordQuarantine(ctx, &QuarantineRecord{}); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed for RecordQuarantine, got %v", err)
	}
	if _, err := store.GetQuarantineRecord(ctx, "any"); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed for GetQuarantineRecord, got %v", err)
	}
	if _, err := store.ListQuarantineRecords(ctx, QuarantineFilter{}); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed for ListQuarantineRecords, got %v", err)
	}
	if _, err := store.UpdateQuarantineStatus(ctx, QuarantineStatusUpdate{ID: "any", NewStatus: StatusDiscarded}); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed for UpdateQuarantineStatus, got %v", err)
	}
}

func TestQuarantine_SchemaMigration_LegacyTableUpgrade(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "legacy_quarantine.db")

	// Step 1: Open raw sql.DB and create a pre-migration cp_telemetry_quarantine table lacking last_failure columns
	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open raw sqlite: %v", err)
	}
	legacyDDL := `
	CREATE TABLE cp_telemetry_quarantine (
		id                   TEXT PRIMARY KEY NOT NULL,
		dedup_key            TEXT UNIQUE NOT NULL,
		source_stream        TEXT NOT NULL DEFAULT '',
		source_sequence      INTEGER,
		source_subject       TEXT NOT NULL DEFAULT '',
		node_id              TEXT NOT NULL DEFAULT '',
		batch_id             TEXT NOT NULL DEFAULT '',
		payload              BLOB NOT NULL,
		failure_category     TEXT NOT NULL,
		failure_reason       TEXT NOT NULL,
		delivery_count       INTEGER NOT NULL DEFAULT 0,
		attempts             INTEGER NOT NULL DEFAULT 1,
		first_quarantined_at TEXT NOT NULL,
		last_quarantined_at  TEXT NOT NULL,
		status               TEXT NOT NULL DEFAULT 'quarantined',
		status_updated_at    TEXT NOT NULL,
		status_reason        TEXT NOT NULL DEFAULT '',
		status_updated_by    TEXT NOT NULL DEFAULT '',
		replay_metadata_json TEXT NOT NULL DEFAULT '{}',
		created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	`
	if _, err := rawDB.Exec(legacyDDL); err != nil {
		t.Fatalf("failed to create legacy table: %v", err)
	}

	// Insert legacy row
	insertLegacy := `
	INSERT INTO cp_telemetry_quarantine (
		id, dedup_key, payload, failure_category, failure_reason,
		first_quarantined_at, last_quarantined_at, status_updated_at
	) VALUES (
		'qrn-legacy-1', 'batch:6:node-1:7:batch-1', X'68656c6c6f',
		'deserialization_error', 'syntax error at line 1',
		'2026-10-26T12:00:00.000000000Z', '2026-10-26T12:00:00.000000000Z', '2026-10-26T12:00:00.000000000Z'
	);
	`
	if _, err := rawDB.Exec(insertLegacy); err != nil {
		t.Fatalf("failed to insert legacy row: %v", err)
	}
	_ = rawDB.Close()

	// Step 2: Open with OpenSQLite, triggering initSchema() migration
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite on legacy database failed: %v", err)
	}
	defer store.Close()

	// Step 3: Fetch migrated row and verify last_failure columns were backfilled
	rec, err := store.GetQuarantineRecord(ctx, "qrn-legacy-1")
	if err != nil {
		t.Fatalf("GetQuarantineRecord on migrated database failed: %v", err)
	}
	if rec.FailureCategory != CategoryDeserialization {
		t.Errorf("initial FailureCategory mismatch: got %s, want %s", rec.FailureCategory, CategoryDeserialization)
	}
	if rec.FailureReason != "syntax error at line 1" {
		t.Errorf("initial FailureReason mismatch: got %q", rec.FailureReason)
	}
	if rec.LastFailureCategory != CategoryDeserialization {
		t.Errorf("backfilled LastFailureCategory mismatch: got %s, want %s", rec.LastFailureCategory, CategoryDeserialization)
	}
	if rec.LastFailureReason != "syntax error at line 1" {
		t.Errorf("backfilled LastFailureReason mismatch: got %q", rec.LastFailureReason)
	}

	// Step 4: Re-open again to verify migration idempotency
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close failed: %v", err)
	}
	store2, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("second OpenSQLite on already-migrated database failed: %v", err)
	}
	_ = store2.Close()
}
