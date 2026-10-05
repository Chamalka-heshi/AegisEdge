package escalation

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// ComputeEscalationID generates a deterministic, idempotent identifier for an escalation record.
// Formula: SHA-256 over IncidentID | NodeID | ActionID | FailureClassification | PolicyVersion | Attempt.
// Returns a prefixed hex string: "esc-" + hex[:32].
// It contains NO random bytes and NO timestamps, ensuring identical inputs produce identical IDs.
func ComputeEscalationID(incidentID, nodeID, actionID string, failureClass types.FailureClassification, policyVersion string, attempt int) string {
	raw := fmt.Sprintf("%s|%s|%s|%s|%s|%d",
		strings.TrimSpace(incidentID),
		strings.TrimSpace(nodeID),
		strings.TrimSpace(actionID),
		strings.TrimSpace(string(failureClass)),
		strings.TrimSpace(policyVersion),
		attempt,
	)
	hash := sha256.Sum256([]byte(raw))
	return "esc-" + hex.EncodeToString(hash[:16])
}
