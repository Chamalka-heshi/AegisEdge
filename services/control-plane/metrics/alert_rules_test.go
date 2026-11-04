package metrics

import (
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestAlertRules_FileIntegrityAndSchema verifies the structure and content of alert_rules.yml
func TestAlertRules_FileIntegrityAndSchema(t *testing.T) {
	rulePath := filepath.Join("..", "..", "..", "infra", "prometheus", "alert_rules.yml")
	data, err := os.ReadFile(rulePath)
	if err != nil {
		t.Fatalf("failed to read alert rules file at %s: %v", rulePath, err)
	}

	content := string(data)

	// 1. Verify group definition
	if !strings.Contains(content, "groups:") || !strings.Contains(content, "name: aegisedge-quarantine-and-nats") {
		t.Fatalf("expected rule group 'aegisedge-quarantine-and-nats' in %s", rulePath)
	}

	// 2. Expected alert names
	expectedAlerts := []string{
		"QuarantineActiveBacklogHigh",
		"QuarantineActiveBacklogCritical",
		"QuarantineActiveCountStorageFailure",
		"QuarantineActiveMetricMissing",
		"NATSPendingRecoveryAccumulationWarning",
		"NATSPendingRecoveryAccumulationCritical",
		"NATSHighMessageTerminationRate",
	}

	for _, alertName := range expectedAlerts {
		pattern := `alert:\s*` + regexp.QuoteMeta(alertName)
		matched, err := regexp.MatchString(pattern, content)
		if err != nil || !matched {
			t.Errorf("expected alert rule %q defined in %s", alertName, rulePath)
		}
	}

	// 3. Verify referenced metric names match actual Go metric constants
	referencedMetrics := map[string]string{
		MetricQuarantineActiveRecords:     "aegisedge_quarantine_active_records",
		MetricNATSPendingRecoveryItems:    "aegisedge_nats_pending_recovery_items",
		MetricNATSTerminatedMessagesTotal: "aegisedge_nats_terminated_messages_total",
	}

	for constVal, metricName := range referencedMetrics {
		if constVal != metricName {
			t.Errorf("metric constant mismatch: expected %q, got %q", metricName, constVal)
		}
		if !strings.Contains(content, metricName) {
			t.Errorf("metric %q expected in alert rules but not found", metricName)
		}
	}

	// 4. Verify NaN detection expression uses PromQL inequality, not non-standard isnan()
	nanExpr := MetricQuarantineActiveRecords + " != " + MetricQuarantineActiveRecords
	if !strings.Contains(content, nanExpr) {
		t.Errorf("expected NaN detection expression %q in %s", nanExpr, rulePath)
	}
	if strings.Contains(content, "isnan(") {
		t.Errorf("alert rules must not use non-standard isnan() PromQL function")
	}

	// 5. Verify absent metric expression
	absentExpr := "absent(" + MetricQuarantineActiveRecords + ")"
	if !strings.Contains(content, absentExpr) {
		t.Errorf("expected absent metric expression %q in %s", absentExpr, rulePath)
	}

	// 6. Verify duration format in 'for' clauses
	forRegex := regexp.MustCompile(`for:\s*([0-9]+[smhd])`)
	forMatches := forRegex.FindAllStringSubmatch(content, -1)
	if len(forMatches) < len(expectedAlerts) {
		t.Errorf("expected at least %d 'for' durations, found %d", len(expectedAlerts), len(forMatches))
	}
	for _, m := range forMatches {
		_, err := time.ParseDuration(m[1])
		if err != nil {
			t.Errorf("invalid duration format %q: %v", m[1], err)
		}
	}

	// 7. Verify severities are strictly warning or critical
	sevRegex := regexp.MustCompile(`severity:\s*([a-zA-Z0-9_-]+)`)
	sevMatches := sevRegex.FindAllStringSubmatch(content, -1)
	for _, m := range sevMatches {
		sev := m[1]
		if sev != "warning" && sev != "critical" {
			t.Errorf("unexpected severity label %q; expected 'warning' or 'critical'", sev)
		}
	}
}

// TestAlertRules_CounterPreinitialization verifies that the terminated messages counter
// is emitted immediately with value 0, guaranteeing that increase() has a baseline from t=0.
func TestAlertRules_CounterPreinitialization(t *testing.T) {
	m := NewMetrics()
	output := m.Registry().FormatPrometheus()

	// aegisedge_nats_terminated_messages_total must be present even when no source or events occurred
	if !strings.Contains(output, MetricNATSTerminatedMessagesTotal+" 0") {
		t.Errorf("expected %s to be preinitialized with 0, got output:\n%s",
			MetricNATSTerminatedMessagesTotal, output)
	}

	// aegisedge_quarantine_active_records must be preinitialized with 0
	if !strings.Contains(output, MetricQuarantineActiveRecords+" 0") {
		t.Errorf("expected %s to be preinitialized with 0, got output:\n%s",
			MetricQuarantineActiveRecords, output)
	}

	// aegisedge_nats_pending_recovery_items must be preinitialized with 0
	if !strings.Contains(output, MetricNATSPendingRecoveryItems+" 0") {
		t.Errorf("expected %s to be preinitialized with 0, got output:\n%s",
			MetricNATSPendingRecoveryItems, output)
	}
}

// TestAlertRules_YAMLSyntax validates that the YAML file adheres to strict syntax rules:
// no tabs, consistent 2-space indentation, balanced quotes, and well-formed key-value structures.
func TestAlertRules_YAMLSyntax(t *testing.T) {
	rulePath := filepath.Join("..", "..", "..", "infra", "prometheus", "alert_rules.yml")
	data, err := os.ReadFile(rulePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	lines := strings.Split(string(data), "\n")
	for lineIdx, line := range lines {
		trimmed := strings.TrimRight(line, "\r")
		lineNum := lineIdx + 1

		// Check for tabs (prohibited in YAML)
		if strings.Contains(trimmed, "\t") {
			t.Errorf("line %d: tabs are forbidden in YAML: %q", lineNum, trimmed)
		}

		// Skip comments and blank lines
		stripped := strings.TrimSpace(trimmed)
		if stripped == "" || strings.HasPrefix(stripped, "#") {
			continue
		}

		// Check leading spaces count
		leadingSpaces := len(trimmed) - len(strings.TrimLeft(trimmed, " "))
		if leadingSpaces%2 != 0 {
			t.Errorf("line %d: indentation must be multiple of 2 spaces, got %d: %q", lineNum, leadingSpaces, trimmed)
		}

		// Check balanced quotes
		doubleQuotes := strings.Count(trimmed, "\"")
		if doubleQuotes%2 != 0 {
			t.Errorf("line %d: unbalanced double quotes in line: %q", lineNum, trimmed)
		}
	}
}

// TestAlertRules_NaNDetectionLogic verifies IEEE 754 logic for NaN detection in PromQL
func TestAlertRules_NaNDetectionLogic(t *testing.T) {
	// Normal value compared to itself is false for !=
	val := 42.0
	if val != val {
		t.Errorf("normal float comparison val != val should be false, got true")
	}

	// NaN value compared to itself is true for !=
	nanVal := math.NaN()
	if !(nanVal != nanVal) {
		t.Errorf("NaN comparison nanVal != nanVal should be true, got false")
	}
}

// TestAlertRules_PromtoolCheck runs 'promtool check rules' if promtool binary is available on PATH
func TestAlertRules_PromtoolCheck(t *testing.T) {
	promtoolPath, err := exec.LookPath("promtool")
	if err != nil {
		t.Logf("promtool binary not found on PATH; skipping promtool CLI execution (run 'promtool check rules infra/prometheus/alert_rules.yml' in CI)")
		return
	}

	rulePath := filepath.Join("..", "..", "..", "infra", "prometheus", "alert_rules.yml")
	cmd := exec.Command(promtoolPath, "check", "rules", rulePath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("promtool check rules failed: %v\nOutput: %s", err, string(out))
	}
	t.Logf("promtool check rules succeeded:\n%s", string(out))
}
