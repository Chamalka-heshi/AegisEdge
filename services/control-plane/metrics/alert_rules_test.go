package metrics

import (
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// AlertRuleGroup represents a Prometheus alerting rule group.
type AlertRuleGroup struct {
	Name  string      `yaml:"name"`
	Rules []AlertRule `yaml:"rules"`
}

// AlertRule represents an individual Prometheus alerting rule definition.
type AlertRule struct {
	Alert       string            `yaml:"alert"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for"`
	Labels      map[string]string `yaml:"labels"`
	Annotations map[string]string `yaml:"annotations"`
}

// AlertRuleFile represents the top-level schema of a Prometheus rule file.
type AlertRuleFile struct {
	Groups []AlertRuleGroup `yaml:"groups"`
}

// PrometheusConfigFile represents the top-level schema of prometheus.yml.
type PrometheusConfigFile struct {
	Global struct {
		ScrapeInterval     string `yaml:"scrape_interval"`
		EvaluationInterval string `yaml:"evaluation_interval"`
	} `yaml:"global"`
	RuleFiles     []string `yaml:"rule_files"`
	ScrapeConfigs []struct {
		JobName       string `yaml:"job_name"`
		MetricsPath   string `yaml:"metrics_path"`
		StaticConfigs []struct {
			Targets []string `yaml:"targets"`
		} `yaml:"static_configs"`
	} `yaml:"scrape_configs"`
}

// TestAlertRules_YAMLUnmarshalAndSchema performs YAML parsing and typed structural validation
// for alert_rules.yml, asserting all alert definitions and exact PromQL expressions.
func TestAlertRules_YAMLUnmarshalAndSchema(t *testing.T) {
	rulePath := filepath.Join("..", "..", "..", "infra", "prometheus", "alert_rules.yml")
	data, err := os.ReadFile(rulePath)
	if err != nil {
		t.Fatalf("failed to read alert rules file at %s: %v", rulePath, err)
	}

	var rf AlertRuleFile
	if err := yaml.Unmarshal(data, &rf); err != nil {
		t.Fatalf("YAML unmarshal failed for %s: %v", rulePath, err)
	}

	if len(rf.Groups) != 1 {
		t.Fatalf("expected exactly 1 rule group, got %d", len(rf.Groups))
	}

	group := rf.Groups[0]
	if group.Name != "aegisedge-quarantine-and-nats" {
		t.Errorf("expected group name 'aegisedge-quarantine-and-nats', got %q", group.Name)
	}

	expectedAlerts := map[string]string{
		"QuarantineActiveBacklogHigh":             "aegisedge_quarantine_active_records > 100",
		"QuarantineActiveBacklogCritical":         "aegisedge_quarantine_active_records > 500",
		"QuarantineActiveCountStorageFailure":     "aegisedge_quarantine_active_records != aegisedge_quarantine_active_records",
		"QuarantineActiveMetricMissing":           "absent(aegisedge_quarantine_active_records)",
		"NATSPendingRecoveryAccumulationWarning":  "aegisedge_nats_pending_recovery_items > 0",
		"NATSPendingRecoveryAccumulationCritical": "aegisedge_nats_pending_recovery_items > 20",
		"NATSHighMessageTerminationRate":          "increase(aegisedge_nats_terminated_messages_total[5m]) > 10",
	}

	seenAlerts := make(map[string]bool)

	if len(group.Rules) != len(expectedAlerts) {
		t.Fatalf("expected %d rules, got %d", len(expectedAlerts), len(group.Rules))
	}

	for _, rule := range group.Rules {
		if rule.Alert == "" {
			t.Errorf("rule has empty alert name: %+v", rule)
			continue
		}
		expectedExpr, ok := expectedAlerts[rule.Alert]
		if !ok {
			t.Errorf("unexpected alert name %q in rules", rule.Alert)
			continue
		}
		if seenAlerts[rule.Alert] {
			t.Errorf("duplicate alert definition for %q", rule.Alert)
			continue
		}
		seenAlerts[rule.Alert] = true

		trimmedExpr := strings.TrimSpace(rule.Expr)
		if trimmedExpr == "" {
			t.Errorf("alert %q has empty expression", rule.Alert)
		} else if trimmedExpr != expectedExpr {
			t.Errorf("alert %q expression mismatch:\nexpected: %q\ngot:      %q",
				rule.Alert, expectedExpr, trimmedExpr)
		}

		if rule.For == "" {
			t.Errorf("alert %q has empty 'for' duration", rule.Alert)
		} else {
			dur, err := time.ParseDuration(rule.For)
			if err != nil || dur <= 0 {
				t.Errorf("alert %q has invalid duration %q: %v", rule.Alert, rule.For, err)
			}
		}

		sev := rule.Labels["severity"]
		if sev != "warning" && sev != "critical" {
			t.Errorf("alert %q has invalid severity %q (expected 'warning' or 'critical')", rule.Alert, sev)
		}

		if strings.TrimSpace(rule.Annotations["summary"]) == "" {
			t.Errorf("alert %q has empty summary annotation", rule.Alert)
		}
		if strings.TrimSpace(rule.Annotations["description"]) == "" {
			t.Errorf("alert %q has empty description annotation", rule.Alert)
		}
	}

	for alertName := range expectedAlerts {
		if !seenAlerts[alertName] {
			t.Errorf("expected alert %q was not found in parsed YAML", alertName)
		}
	}

	// Verify metric constants appear in expressions
	content := string(data)
	for _, metricName := range []string{
		MetricQuarantineActiveRecords,
		MetricNATSPendingRecoveryItems,
		MetricNATSTerminatedMessagesTotal,
	} {
		if !strings.Contains(content, metricName) {
			t.Errorf("expected metric %q referenced in rule file expressions", metricName)
		}
	}

	// Verify IEEE 754 NaN detection idiom (metric != metric)
	nanExpr := MetricQuarantineActiveRecords + " != " + MetricQuarantineActiveRecords
	if !strings.Contains(content, nanExpr) {
		t.Errorf("expected NaN detection expression %q in %s", nanExpr, rulePath)
	}
	if strings.Contains(content, "isnan(") {
		t.Errorf("alert rules must not use non-standard isnan() PromQL function")
	}

	// Verify absent metric expression
	absentExpr := "absent(" + MetricQuarantineActiveRecords + ")"
	if !strings.Contains(content, absentExpr) {
		t.Errorf("expected absent metric expression %q in %s", absentExpr, rulePath)
	}
}

// TestPrometheusConfig_YAMLUnmarshalAndSchema performs YAML parsing and typed structural validation
// for prometheus.yml.
func TestPrometheusConfig_YAMLUnmarshalAndSchema(t *testing.T) {
	configPath := filepath.Join("..", "..", "..", "infra", "prometheus", "prometheus.yml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read prometheus config file at %s: %v", configPath, err)
	}

	var cfg PrometheusConfigFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("YAML unmarshal failed for %s: %v", configPath, err)
	}

	scrapeDur, err := time.ParseDuration(cfg.Global.ScrapeInterval)
	if err != nil || scrapeDur <= 0 {
		t.Errorf("invalid scrape_interval %q: %v", cfg.Global.ScrapeInterval, err)
	}

	evalDur, err := time.ParseDuration(cfg.Global.EvaluationInterval)
	if err != nil || evalDur <= 0 {
		t.Errorf("invalid evaluation_interval %q: %v", cfg.Global.EvaluationInterval, err)
	}

	foundRuleFile := false
	for _, rf := range cfg.RuleFiles {
		if rf == "alert_rules.yml" {
			foundRuleFile = true
			break
		}
	}
	if !foundRuleFile {
		t.Errorf("expected 'alert_rules.yml' in rule_files, got %v", cfg.RuleFiles)
	}

	foundJob := false
	for _, sc := range cfg.ScrapeConfigs {
		if sc.JobName == "aegisedge-control-plane" {
			foundJob = true
			if sc.MetricsPath != "/metrics" {
				t.Errorf("expected metrics_path '/metrics', got %q", sc.MetricsPath)
			}
			foundTarget := false
			for _, stc := range sc.StaticConfigs {
				for _, tgt := range stc.Targets {
					if tgt == "127.0.0.1:8080" {
						foundTarget = true
						break
					}
				}
			}
			if !foundTarget {
				t.Errorf("expected target '127.0.0.1:8080' in scrape config")
			}
		}
	}
	if !foundJob {
		t.Errorf("expected scrape job 'aegisedge-control-plane' in scrape_configs")
	}
}

// TestAlertRules_CounterPreinitialization verifies that the terminated messages counter
// is emitted immediately with value 0, guaranteeing that increase() has a baseline from t=0.
func TestAlertRules_CounterPreinitialization(t *testing.T) {
	m := NewMetrics()
	output := m.Registry().FormatPrometheus()

	if !strings.Contains(output, MetricNATSTerminatedMessagesTotal+" 0") {
		t.Errorf("expected %s to be preinitialized with 0, got output:\n%s",
			MetricNATSTerminatedMessagesTotal, output)
	}
	if !strings.Contains(output, MetricQuarantineActiveRecords+" 0") {
		t.Errorf("expected %s to be preinitialized with 0, got output:\n%s",
			MetricQuarantineActiveRecords, output)
	}
	if !strings.Contains(output, MetricNATSPendingRecoveryItems+" 0") {
		t.Errorf("expected %s to be preinitialized with 0, got output:\n%s",
			MetricNATSPendingRecoveryItems, output)
	}
}

// TestAlertRules_NaNDetectionLogic verifies IEEE 754 logic for NaN detection in PromQL
func TestAlertRules_NaNDetectionLogic(t *testing.T) {
	val := 42.0
	if val != val {
		t.Errorf("normal float comparison val != val should be false, got true")
	}

	nanVal := math.NaN()
	if !(nanVal != nanVal) {
		t.Errorf("NaN comparison nanVal != nanVal should be true, got false")
	}
}

// TestAlertRules_PromtoolCheck runs 'promtool check rules' and 'promtool check config'
// if the promtool binary is available on PATH. If missing, it explicitly marks the test skipped.
func TestAlertRules_PromtoolCheck(t *testing.T) {
	promtoolPath, err := exec.LookPath("promtool")
	if err != nil {
		t.Skip("promtool binary not found on PATH; skipping CLI validation (run 'promtool check rules infra/prometheus/alert_rules.yml')")
		return
	}

	rulePath := filepath.Join("..", "..", "..", "infra", "prometheus", "alert_rules.yml")
	cmdRules := exec.Command(promtoolPath, "check", "rules", rulePath)
	outRules, err := cmdRules.CombinedOutput()
	if err != nil {
		t.Fatalf("promtool check rules failed: %v\nOutput: %s", err, string(outRules))
	}
	t.Logf("promtool check rules succeeded:\n%s", string(outRules))

	cfgPath := filepath.Join("..", "..", "..", "infra", "prometheus", "prometheus.yml")
	cmdCfg := exec.Command(promtoolPath, "check", "config", cfgPath)
	outCfg, err := cmdCfg.CombinedOutput()
	if err != nil {
		t.Fatalf("promtool check config failed: %v\nOutput: %s", err, string(outCfg))
	}
	t.Logf("promtool check config succeeded:\n%s", string(outCfg))
}
