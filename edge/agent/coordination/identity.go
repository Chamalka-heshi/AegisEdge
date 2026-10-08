package coordination

import (
	"context"
	"fmt"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
)

// ResolveNodeIdentity safely retrieves or initializes the persistent node identity.
// If configuredID is provided, it is validated and persisted if no identity exists yet.
// If configuredID is empty and no stored identity exists, a cryptographically secure
// random node ID is generated and durably stored.
// If an identity already exists in local storage, that identity is preserved and returned.
func ResolveNodeIdentity(ctx context.Context, store storage.NodeIdentityStore, configuredID string) (string, error) {
	if store == nil {
		return "", fmt.Errorf("storage is required for node identity resolution")
	}

	nodeID, err := store.GetOrCreateNodeIdentity(ctx, configuredID)
	if err != nil {
		return "", fmt.Errorf("failed to resolve node identity: %w", err)
	}

	return nodeID, nil
}
