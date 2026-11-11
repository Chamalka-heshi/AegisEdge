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
	RuleFiles []string `yaml:"rule_files"`
	Alerting  struct {
		Alertmanagers []struct {
			StaticConfigs []struct {
				Targets []string `yaml:"targets"`
			} `yaml:"static_configs"`
		} `yaml:"alertmanagers"`
	} `yaml:"alerting"`
	ScrapeConfigs []struct {
		JobName       string `yaml:"job_name"`
		MetricsPath   string `yaml:"metrics_path"`
		StaticConfigs []struct {
			Targets []string `yaml:"targets"`
		} `yaml:"static_configs"`
	} `yaml:"scrape_configs"`
}

// InhibitRule represents an Alertmanager alert inhibition rule.
type InhibitRule struct {
	TargetMatchers []string `yaml:"target_matchers"`
	SourceMatchers []string `yaml:"source_matchers"`
	Equal          []string `yaml:"equal"`
}

// AlertmanagerConfigFile represents the schema of alertmanager.yml.
type AlertmanagerConfigFile struct {
	Global struct {
		ResolveTimeout string `yaml:"resolve_timeout"`
	} `yaml:"global"`
	Route struct {
		Receiver       string   `yaml:"receiver"`
		GroupBy        []string `yaml:"group_by"`
		GroupWait      string   `yaml:"group_wait"`
		GroupInterval  string   `yaml:"group_interval"`
		RepeatInterval string   `yaml:"repeat_interval"`
		Routes         []struct {
			Receiver       string   `yaml:"receiver"`
			Matchers       []string `yaml:"matchers"`
			RepeatInterval string   `yaml:"repeat_interval"`
		} `yaml:"routes"`
	} `yaml:"route"`
	Receivers []struct {
		Name           string `yaml:"name"`
		WebhookConfigs []struct {
			URL          string `yaml:"url"`
			SendResolved bool   `yaml:"send_resolved"`
			HTTPConfig   struct {
				BearerTokenFile string `yaml:"bearer_token_file"`
			} `yaml:"http_config"`
		} `yaml:"webhook_configs"`
	} `yaml:"receivers"`
	InhibitRules []InhibitRule `yaml:"inhibit_rules"`
}

// PrometheusRuleTestFile represents the schema of alert_rules_test.yml for promtool.
type PrometheusRuleTestFile struct {
	RuleFiles          []string `yaml:"rule_files"`
	EvaluationInterval string   `yaml:"evaluation_interval"`
	Tests              []struct {
		Interval    string `yaml:"interval"`
		InputSeries []struct {
			Series string `yaml:"series"`
			Values string `yaml:"values"`
		} `yaml:"input_series"`
		AlertRuleTest []struct {
			EvalTime  string `yaml:"eval_time"`
			Alertname string `yaml:"alertname"`
			ExpAlerts []struct {
				ExpLabels      map[string]string `yaml:"exp_labels"`
				ExpAnnotations map[string]string `yaml:"exp_annotations"`
			} `yaml:"exp_alerts"`
		} `yaml:"alert_rule_test"`
	} `yaml:"tests"`
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

	foundAlertmanager := false
	for _, am := range cfg.Alerting.Alertmanagers {
		for _, sc := range am.StaticConfigs {
			for _, tgt := range sc.Targets {
				if tgt == "127.0.0.1:9093" {
					foundAlertmanager = true
					break
				}
			}
		}
	}
	if !foundAlertmanager {
		t.Errorf("expected alertmanager target '127.0.0.1:9093' in prometheus.yml alerting configuration")
	}

	// Verify strict isolation: production prometheus.yml must not contain test rules or test scrape jobs
	for _, rf := range cfg.RuleFiles {
		if rf != "alert_rules.yml" {
			t.Errorf("production prometheus.yml must not contain test rule file %q", rf)
		}
	}
	for _, sc := range cfg.ScrapeConfigs {
		if sc.JobName != "aegisedge-control-plane" {
			t.Errorf("production prometheus.yml must not contain test scrape job %q", sc.JobName)
		}
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

// TestAlertRulesTestFile_YAMLUnmarshalAndSchema performs typed schema validation for alert_rules_test.yml,
// verifying that all 7 alerts have behavioral test scenarios covering firing and resolution.
func TestAlertRulesTestFile_YAMLUnmarshalAndSchema(t *testing.T) {
	testPath := filepath.Join("..", "..", "..", "infra", "prometheus", "alert_rules_test.yml")
	data, err := os.ReadFile(testPath)
	if err != nil {
		t.Fatalf("failed to read alert rules test file at %s: %v", testPath, err)
	}

	var tf PrometheusRuleTestFile
	if err := yaml.Unmarshal(data, &tf); err != nil {
		t.Fatalf("YAML unmarshal failed for %s: %v", testPath, err)
	}

	foundRuleFile := false
	for _, rf := range tf.RuleFiles {
		if rf == "alert_rules.yml" {
			foundRuleFile = true
			break
		}
	}
	if !foundRuleFile {
		t.Errorf("expected 'alert_rules.yml' in rule_files of %s, got %v", testPath, tf.RuleFiles)
	}

	evalDur, err := time.ParseDuration(tf.EvaluationInterval)
	if err != nil || evalDur <= 0 {
		t.Errorf("invalid evaluation_interval %q in %s: %v", tf.EvaluationInterval, testPath, err)
	}

	expectedAlerts := map[string]bool{
		"QuarantineActiveBacklogHigh":             false,
		"QuarantineActiveBacklogCritical":         false,
		"QuarantineActiveCountStorageFailure":     false,
		"QuarantineActiveMetricMissing":           false,
		"NATSPendingRecoveryAccumulationWarning":  false,
		"NATSPendingRecoveryAccumulationCritical": false,
		"NATSHighMessageTerminationRate":          false,
	}

	if len(tf.Tests) == 0 {
		t.Fatalf("expected non-empty test suites in %s", testPath)
	}

	for i, testCase := range tf.Tests {
		intervalDur, err := time.ParseDuration(testCase.Interval)
		if err != nil || intervalDur <= 0 {
			t.Errorf("test[%d] has invalid interval %q: %v", i, testCase.Interval, err)
		}
		if len(testCase.InputSeries) == 0 {
			t.Errorf("test[%d] has no input_series", i)
		}
		for sIdx, is := range testCase.InputSeries {
			if strings.TrimSpace(is.Series) == "" {
				t.Errorf("test[%d].input_series[%d] has empty series name", i, sIdx)
			}
			if strings.TrimSpace(is.Values) == "" {
				t.Errorf("test[%d].input_series[%d] has empty values", i, sIdx)
			}
		}
		for aIdx, art := range testCase.AlertRuleTest {
			if strings.TrimSpace(art.EvalTime) == "" {
				t.Errorf("test[%d].alert_rule_test[%d] has empty eval_time", i, aIdx)
			} else {
				if _, err := time.ParseDuration(art.EvalTime); err != nil {
					t.Errorf("test[%d].alert_rule_test[%d] has invalid eval_time %q: %v", i, aIdx, art.EvalTime, err)
				}
			}
			if strings.TrimSpace(art.Alertname) == "" {
				t.Errorf("test[%d].alert_rule_test[%d] has empty alertname", i, aIdx)
			} else {
				if _, ok := expectedAlerts[art.Alertname]; ok {
					expectedAlerts[art.Alertname] = true
				} else {
					t.Errorf("test[%d].alert_rule_test[%d] tests unexpected alertname %q", i, aIdx, art.Alertname)
				}
			}
			for expIdx, exp := range art.ExpAlerts {
				sev := exp.ExpLabels["severity"]
				if sev != "warning" && sev != "critical" {
					t.Errorf("test[%d].alert_rule_test[%d].exp_alerts[%d] has invalid severity %q", i, aIdx, expIdx, sev)
				}
				if strings.TrimSpace(exp.ExpAnnotations["summary"]) == "" {
					t.Errorf("test[%d].alert_rule_test[%d].exp_alerts[%d] has empty summary", i, aIdx, expIdx)
				}
				if strings.TrimSpace(exp.ExpAnnotations["description"]) == "" {
					t.Errorf("test[%d].alert_rule_test[%d].exp_alerts[%d] has empty description", i, aIdx, expIdx)
				}
			}
		}
	}

	for alertName, tested := range expectedAlerts {
		if !tested {
			t.Errorf("expected alert %q is not covered by any behavioral test scenario in %s", alertName, testPath)
		}
	}
}

// TestPrometheusDockerConfig_YAMLUnmarshalAndSchema performs typed schema validation for prometheus.docker.yml.
func TestPrometheusDockerConfig_YAMLUnmarshalAndSchema(t *testing.T) {
	configPath := filepath.Join("..", "..", "..", "infra", "prometheus", "prometheus.docker.yml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read prometheus docker config file at %s: %v", configPath, err)
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
					if tgt == "host.docker.internal:8080" {
						foundTarget = true
						break
					}
				}
			}
			if !foundTarget {
				t.Errorf("expected target 'host.docker.internal:8080' in scrape config")
			}
		}
	}
	if !foundJob {
		t.Errorf("expected scrape job 'aegisedge-control-plane' in scrape_configs")
	}

	foundAlertmanager := false
	for _, am := range cfg.Alerting.Alertmanagers {
		for _, sc := range am.StaticConfigs {
			for _, tgt := range sc.Targets {
				if tgt == "alertmanager:9093" {
					foundAlertmanager = true
					break
				}
			}
		}
	}
	if !foundAlertmanager {
		t.Errorf("expected alertmanager target 'alertmanager:9093' in prometheus.docker.yml alerting configuration")
	}

	// Verify strict isolation: production prometheus.docker.yml must not contain test rules or test scrape jobs
	for _, rf := range cfg.RuleFiles {
		if rf != "alert_rules.yml" {
			t.Errorf("production prometheus.docker.yml must not contain test rule file %q", rf)
		}
	}
	for _, sc := range cfg.ScrapeConfigs {
		if sc.JobName != "aegisedge-control-plane" {
			t.Errorf("production prometheus.docker.yml must not contain test scrape job %q", sc.JobName)
		}
	}
}

// TestPrometheusTestConfig_YAMLUnmarshalAndSchema performs typed schema validation for prometheus.test.yml,
// verifying that test rules and lifecycle scrape targets are isolated to the test configuration.
func TestPrometheusTestConfig_YAMLUnmarshalAndSchema(t *testing.T) {
	configPath := filepath.Join("..", "..", "..", "infra", "prometheus", "prometheus.test.yml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read prometheus test config file at %s: %v", configPath, err)
	}

	var cfg PrometheusConfigFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("YAML unmarshal failed for %s: %v", configPath, err)
	}

	foundProdRules := false
	foundTestRules := false
	for _, rf := range cfg.RuleFiles {
		if rf == "alert_rules.yml" {
			foundProdRules = true
		}
		if rf == "alert_rules_lifecycle_test.yml" {
			foundTestRules = true
		}
	}
	if !foundProdRules {
		t.Errorf("expected 'alert_rules.yml' in test rule_files")
	}
	if !foundTestRules {
		t.Errorf("expected 'alert_rules_lifecycle_test.yml' in test rule_files")
	}

	foundTestScrapeJob := false
	for _, sc := range cfg.ScrapeConfigs {
		if sc.JobName == "aegisedge-lifecycle-test" {
			foundTestScrapeJob = true
			if sc.MetricsPath != "/test-metrics" {
				t.Errorf("expected metrics_path '/test-metrics', got %q", sc.MetricsPath)
			}
		}
	}
	if !foundTestScrapeJob {
		t.Errorf("expected scrape job 'aegisedge-lifecycle-test' in test scrape_configs")
	}

	foundAlertmanager := false
	for _, am := range cfg.Alerting.Alertmanagers {
		for _, sc := range am.StaticConfigs {
			for _, tgt := range sc.Targets {
				if tgt == "alertmanager:9093" {
					foundAlertmanager = true
					break
				}
			}
		}
	}
	if !foundAlertmanager {
		t.Errorf("expected alertmanager target 'alertmanager:9093' in prometheus.test.yml alerting configuration")
	}
}

// TestAlertmanagerConfig_ProductionIsolation performs typed schema validation for production alertmanager.yml,
// asserting production inhibition rules, presence of local-development-sink, and zero synthetic test rules.
func TestAlertmanagerConfig_ProductionIsolation(t *testing.T) {
	configPath := filepath.Join("..", "..", "..", "infra", "prometheus", "alertmanager.yml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read alertmanager config file at %s: %v", configPath, err)
	}

	var cfg AlertmanagerConfigFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("YAML unmarshal failed for %s: %v", configPath, err)
	}

	resolveTimeout, err := time.ParseDuration(cfg.Global.ResolveTimeout)
	if err != nil || resolveTimeout <= 0 {
		t.Errorf("invalid resolve_timeout %q: %v", cfg.Global.ResolveTimeout, err)
	}

	if cfg.Route.Receiver != "local-development-sink" {
		t.Errorf("expected root receiver 'local-development-sink', got %q", cfg.Route.Receiver)
	}

	if len(cfg.Receivers) != 1 || cfg.Receivers[0].Name != "local-development-sink" {
		t.Fatalf("expected exactly 1 receiver 'local-development-sink', got %d receivers", len(cfg.Receivers))
	}
	if len(cfg.Receivers[0].WebhookConfigs) != 0 {
		t.Errorf("expected 0 webhook_configs on local-development-sink, got %d", len(cfg.Receivers[0].WebhookConfigs))
	}

	// Verify production inhibition rules
	if len(cfg.InhibitRules) != 2 {
		t.Fatalf("expected exactly 2 production inhibit rules, got %d", len(cfg.InhibitRules))
	}

	// Rule 1: Quarantine Critical -> Warning with equal: [job, instance]
	r1 := cfg.InhibitRules[0]
	if !containsMatcher(r1.TargetMatchers, "alertname = \"QuarantineActiveBacklogHigh\"") ||
		!containsMatcher(r1.TargetMatchers, "severity = \"warning\"") ||
		!containsMatcher(r1.SourceMatchers, "alertname = \"QuarantineActiveBacklogCritical\"") ||
		!containsMatcher(r1.SourceMatchers, "severity = \"critical\"") {
		t.Errorf("unexpected quarantine inhibition rule matchers: %+v", r1)
	}
	if !slicesEqual(r1.Equal, []string{"job", "instance"}) {
		t.Errorf("expected quarantine inhibition equal: [job, instance], got %+v", r1.Equal)
	}

	// Rule 2: NATS Critical -> Warning with equal: [job, instance]
	r2 := cfg.InhibitRules[1]
	if !containsMatcher(r2.TargetMatchers, "alertname = \"NATSPendingRecoveryAccumulationWarning\"") ||
		!containsMatcher(r2.TargetMatchers, "severity = \"warning\"") ||
		!containsMatcher(r2.SourceMatchers, "alertname = \"NATSPendingRecoveryAccumulationCritical\"") ||
		!containsMatcher(r2.SourceMatchers, "severity = \"critical\"") {
		t.Errorf("unexpected NATS inhibition rule matchers: %+v", r2)
	}
	if !slicesEqual(r2.Equal, []string{"job", "instance"}) {
		t.Errorf("expected NATS inhibition equal: [job, instance], got %+v", r2.Equal)
	}

	// Strict production isolation: zero synthetic lifecycle test rules
	for i, ir := range cfg.InhibitRules {
		for _, m := range ir.TargetMatchers {
			if strings.Contains(m, "AegisEdgeE2E") {
				t.Errorf("synthetic test rule leaked into production alertmanager.yml at inhibit_rules[%d]: %s", i, m)
			}
		}
		for _, m := range ir.SourceMatchers {
			if strings.Contains(m, "AegisEdgeE2E") {
				t.Errorf("synthetic test rule leaked into production alertmanager.yml at inhibit_rules[%d]: %s", i, m)
			}
		}
	}
}

// TestAlertmanagerTestConfig_Schema performs typed schema validation for alertmanager.test.yml,
// asserting webhook configurations, bearer token authentication, test_run_id grouping, and synthetic inhibition rules.
func TestAlertmanagerTestConfig_Schema(t *testing.T) {
	configPath := filepath.Join("..", "..", "..", "infra", "prometheus", "alertmanager.test.yml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read test alertmanager config file at %s: %v", configPath, err)
	}

	var cfg AlertmanagerConfigFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("YAML unmarshal failed for %s: %v", configPath, err)
	}

	if cfg.Route.Receiver != "default-receiver" {
		t.Errorf("expected root receiver 'default-receiver', got %q", cfg.Route.Receiver)
	}

	// Verify test_run_id is included in group_by to prevent cross-run alert batching
	foundTestRunID := false
	for _, gb := range cfg.Route.GroupBy {
		if gb == "test_run_id" {
			foundTestRunID = true
			break
		}
	}
	if !foundTestRunID {
		t.Errorf("expected 'test_run_id' in route.group_by for alertmanager.test.yml, got %+v", cfg.Route.GroupBy)
	}

	expectedReceivers := map[string]bool{
		"default-receiver":  false,
		"critical-receiver": false,
		"warning-receiver":  false,
	}

	for _, r := range cfg.Receivers {
		if _, ok := expectedReceivers[r.Name]; ok {
			expectedReceivers[r.Name] = true
		} else {
			t.Errorf("unexpected receiver %q in alertmanager.test.yml", r.Name)
		}
		if len(r.WebhookConfigs) == 0 {
			t.Errorf("receiver %q has no webhook_configs", r.Name)
		}
		for _, wh := range r.WebhookConfigs {
			if wh.URL == "" {
				t.Errorf("receiver %q has empty webhook URL", r.Name)
			}
			if !wh.SendResolved {
				t.Errorf("receiver %q expected send_resolved: true", r.Name)
			}
			if wh.HTTPConfig.BearerTokenFile != "/etc/alertmanager/secrets/test_token" {
				t.Errorf("receiver %q expected bearer_token_file '/etc/alertmanager/secrets/test_token', got %q", r.Name, wh.HTTPConfig.BearerTokenFile)
			}
		}
	}

	for rName, found := range expectedReceivers {
		if !found {
			t.Errorf("expected receiver %q not found in alertmanager.test.yml", rName)
		}
	}

	// Verify test inhibition rules: 2 production rules + 1 synthetic rule = 3 rules
	if len(cfg.InhibitRules) != 3 {
		t.Fatalf("expected exactly 3 inhibit rules in alertmanager.test.yml, got %d", len(cfg.InhibitRules))
	}

	// Rule 3: Synthetic Lifecycle Test Inhibit Rule with equal: [job, tier, test_run_id]
	r3 := cfg.InhibitRules[2]
	if !containsMatcher(r3.TargetMatchers, "alertname = \"AegisEdgeE2ELifecycleWarning\"") ||
		!containsMatcher(r3.TargetMatchers, "severity = \"warning\"") ||
		!containsMatcher(r3.SourceMatchers, "alertname = \"AegisEdgeE2ELifecycleCritical\"") ||
		!containsMatcher(r3.SourceMatchers, "severity = \"critical\"") {
		t.Errorf("unexpected synthetic inhibition rule matchers: %+v", r3)
	}
	if !slicesEqual(r3.Equal, []string{"job", "tier", "test_run_id"}) {
		t.Errorf("expected synthetic inhibition equal: [job, tier, test_run_id], got %+v", r3.Equal)
	}
}

func containsMatcher(matchers []string, target string) bool {
	for _, m := range matchers {
		if strings.TrimSpace(m) == target {
			return true
		}
	}
	return false
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestAlertRules_PromtoolCheck runs 'promtool check rules', 'promtool check config',
// and 'promtool test rules' if the promtool binary is available on PATH.
// If missing, it explicitly marks the test skipped.
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

	dockerCfgPath := filepath.Join("..", "..", "..", "infra", "prometheus", "prometheus.docker.yml")
	cmdDockerCfg := exec.Command(promtoolPath, "check", "config", dockerCfgPath)
	outDockerCfg, err := cmdDockerCfg.CombinedOutput()
	if err != nil {
		t.Fatalf("promtool check config (docker) failed: %v\nOutput: %s", err, string(outDockerCfg))
	}
	t.Logf("promtool check config (docker) succeeded:\n%s", string(outDockerCfg))

	testRulesPath := filepath.Join("..", "..", "..", "infra", "prometheus", "alert_rules_test.yml")
	cmdTestRules := exec.Command(promtoolPath, "test", "rules", testRulesPath)
	outTestRules, err := cmdTestRules.CombinedOutput()
	if err != nil {
		t.Fatalf("promtool test rules failed: %v\nOutput: %s", err, string(outTestRules))
	}
	t.Logf("promtool test rules succeeded:\n%s", string(outTestRules))
}
