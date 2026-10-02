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
	"eval(", "eval ", "exec(", "exec ", "system(", "system ", "systemctl", "mkfs", "dd ", "chmod", "chown",
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

// AllowlistedActions returns a copy of all recognized allowlisted action types in deterministic order.
func AllowlistedActions() []types.MitigationActionType {
	return []types.MitigationActionType{
		types.ActionSimulatedAlert,
		types.ActionSimulatedThrottle,
		types.ActionSimulatedRestart,
		types.ActionSimulatedIsolate,
	}
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

// IsValidTarget verifies that the target identifier adheres to safe naming conventions.
// It allows alphanumeric names, underscores, hyphens, and colons (e.g., "telemetry_generator", "service:collector", "node-local").
// It strictly rejects whitespace, shell metacharacters, slashes, backslashes, and command executables.
func IsValidTarget(target string) bool {
	t := strings.TrimSpace(target)
	if t == "" || len(t) > 64 {
		return false
	}
	// Target cannot contain whitespace
	if strings.ContainsAny(t, " \t\n\r") {
		return false
	}
	// Check against dangerous keywords and shell characters
	if ContainsDangerousPayload(t) {
		return false
	}
	// Prohibited command names even if without symbols
	lower := strings.ToLower(t)
	prohibitedCommands := []string{
		"bash", "sh", "zsh", "cmd", "powershell", "powershell.exe",
		"kill", "pkill", "killall", "rm", "rmdir", "shutdown", "reboot",
		"halt", "poweroff", "init", "systemctl", "service", "eval", "exec",
	}
	for _, cmd := range prohibitedCommands {
		if lower == cmd {
			return false
		}
	}
	// Must only contain safe identifier runes: [a-zA-Z0-9_\-:]
	for _, r := range t {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == ':') {
			return false
		}
	}
	return true
}
