package response

import (
	"errors"
	"strings"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common allowlist and validation domain errors.
var (
	ErrActionNotAllowlisted = errors.New("action type is not on the safety allowlist")
	ErrEmptyActionType      = errors.New("action type cannot be empty")
	ErrDangerousPayload     = errors.New("input contains prohibited shell or executable characters")
)

// allowlistedActions defines the closed runtime allowlist backed by strongly typed action identifiers.
// In Phase 6, all allowlisted actions are strictly safe simulations.
var allowlistedActions = map[types.MitigationActionType]struct{}{
	types.ActionSimulatedAlert:    {},
	types.ActionSimulatedThrottle: {},
	types.ActionSimulatedRestart:  {},
	types.ActionSimulatedIsolate:  {},
}

// dangerousKeywords contains substrings associated with shell injection and destructive operations.
var dangerousKeywords = []string{
	"rm ", "rmdir", "kill", "pkill", "killall", "shutdown", "reboot", "halt", "poweroff",
	"/bin/sh", "/bin/bash", "cmd.exe", "powershell", "powershell.exe",
	"eval", "exec", "system", "mkfs", "dd ", "chmod", "chown",
	"curl", "wget", "nc ", "netcat", "iptables", "nftables", "ufw",
}

// dangerousShellChars contains characters commonly used for command chaining, redirection, or expansion.
var dangerousShellChars = []string{
	"|", ";", "&", "`", "$", "(", ")", "{", "}", "<", ">", "\n", "\r", "\x00",
}

// IsAllowlisted checks whether an action type belongs to the closed runtime allowlist backed by strongly typed action identifiers.
func IsAllowlisted(actionType types.MitigationActionType) bool {
	if strings.TrimSpace(string(actionType)) == "" {
		return false
	}
	_, ok := allowlistedActions[actionType]
	return ok
}

// IsAllowlistedString checks whether a raw string matches an allowlisted action type.
func IsAllowlistedString(action string) bool {
	return IsAllowlisted(types.MitigationActionType(action))
}

// AllowlistedActions returns a copy of all recognized allowlisted action types.
func AllowlistedActions() []types.MitigationActionType {
	actions := make([]types.MitigationActionType, 0, len(allowlistedActions))
	for a := range allowlistedActions {
		actions = append(actions, a)
	}
	return actions
}

// ContainsDangerousPayload scans an input string for shell metacharacters and prohibited command keywords.
// Returns true if any prohibited token is discovered.
func ContainsDangerousPayload(s string) bool {
	lower := strings.ToLower(s)

	for _, ch := range dangerousShellChars {
		if strings.Contains(s, ch) {
			return true
		}
	}

	for _, kw := range dangerousKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}

	return false
}
