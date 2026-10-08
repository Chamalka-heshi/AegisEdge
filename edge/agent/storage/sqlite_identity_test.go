package storage_test

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
)

func TestNodeIdentity_MigrationV10_FreshAndPersistent(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "identity_test.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// Initial query should return ErrNodeIdentityNotFound
	_, err = store.GetNodeIdentity(ctx)
	if err != storage.ErrNodeIdentityNotFound {
		t.Fatalf("expected ErrNodeIdentityNotFound, got: %v", err)
	}

	// GetOrCreate with defaultID
	id1, err := store.GetOrCreateNodeIdentity(ctx, "node-edge-test-01")
	if err != nil {
		t.Fatalf("GetOrCreateNodeIdentity failed: %v", err)
	}
	if id1 != "node-edge-test-01" {
		t.Fatalf("expected node-edge-test-01, got %s", id1)
	}

	// Repeated GetOrCreate must return identical ID
	id2, err := store.GetOrCreateNodeIdentity(ctx, "different-id")
	if err != nil {
		t.Fatalf("GetOrCreateNodeIdentity failed on second call: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("node identity changed across calls: expected %s, got %s", id1, id2)
	}

	// Reopen database (simulate restart) - must persist identical identity
	_ = store.Close()
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store2.Close()

	id3, err := store2.GetNodeIdentity(ctx)
	if err != nil {
		t.Fatalf("GetNodeIdentity after reopen failed: %v", err)
	}
	if id3 != id1 {
		t.Fatalf("node identity not preserved across reopen: expected %s, got %s", id1, id3)
	}
}

func TestNodeIdentity_RandomGeneration(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "random_id.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// Empty defaultID generates random node-<hex>
	id, err := store.GetOrCreateNodeIdentity(ctx, "")
	if err != nil {
		t.Fatalf("GetOrCreateNodeIdentity with empty default failed: %v", err)
	}
	if !strings.HasPrefix(id, "node-") {
		t.Fatalf("expected prefix node-, got %s", id)
	}
	if len(id) != 21 { // "node-" (5) + 16 hex chars (16)
		t.Fatalf("expected length 21 for node identity, got %d (%s)", len(id), id)
	}
}

func TestNodeIdentity_ValidationAndSecurity(t *testing.T) {
	invalidIDs := []string{
		"",
		"a",
		"ab",
		"node/with/slash",
		"node\\with\\backslash",
		"node with spaces",
		"node;rm -rf",
		"node$var",
		"node-secret-123",
		"node-password-xyz",
		"node-token-abc",
	}

	for _, inv := range invalidIDs {
		if err := storage.ValidateNodeID(inv); err == nil {
			t.Errorf("expected ValidateNodeID to reject %q, got nil", inv)
		}
	}

	validIDs := []string{
		"node-1",
		"edge_node_01",
		"node-edge-001",
		"NODE-TEST-123",
	}

	for _, v := range validIDs {
		if err := storage.ValidateNodeID(v); err != nil {
			t.Errorf("expected ValidateNodeID to accept %q, got: %v", v, err)
		}
	}
}

func TestNodeIdentity_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "concurrent_id.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	var wg sync.WaitGroup
	results := make([]string, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id, err := store.GetOrCreateNodeIdentity(ctx, "concurrent-node-01")
			if err == nil {
				results[idx] = id
			}
		}(i)
	}

	wg.Wait()

	first := results[0]
	if first == "" {
		t.Fatal("expected non-empty node identity")
	}
	for i, r := range results {
		if r != first {
			t.Errorf("goroutine %d got different ID: %s vs %s", i, r, first)
		}
	}
}
