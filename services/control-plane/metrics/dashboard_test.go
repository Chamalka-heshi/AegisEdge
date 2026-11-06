package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// GrafanaDashboard represents the top-level schema of a Grafana dashboard JSON file.
type GrafanaDashboard struct {
	ID            *int           `json:"id"`
	UID           string         `json:"uid"`
	Title         string         `json:"title"`
	Tags          []string       `json:"tags"`
	SchemaVersion int            `json:"schemaVersion"`
	Refresh       string         `json:"refresh"`
	Panels        []GrafanaPanel `json:"panels"`
}

// GrafanaPanel represents an individual dashboard panel or row.
type GrafanaPanel struct {
	ID          int             `json:"id"`
	Title       string          `json:"title"`
	Type        string          `json:"type"`
	Description string          `json:"description"`
	Targets     []GrafanaTarget `json:"targets"`
	GridPos     struct {
		H int `json:"h"`
		W int `json:"w"`
		X int `json:"x"`
		Y int `json:"y"`
	} `json:"gridPos"`
	FieldConfig struct {
		Defaults struct {
			Mappings []map[string]interface{} `json:"mappings"`
		} `json:"defaults"`
	} `json:"fieldConfig"`
}

// GrafanaTarget represents a query target inside a panel.
type GrafanaTarget struct {
	Expr         string `json:"expr"`
	LegendFormat string `json:"legendFormat"`
	RefID        string `json:"refId"`
}

// GrafanaDatasourceConfig represents the YAML structure of a Grafana datasource provisioning file.
type GrafanaDatasourceConfig struct {
	APIVersion  int `yaml:"apiVersion"`
	Datasources []struct {
		Name      string `yaml:"name"`
		Type      string `yaml:"type"`
		Access    string `yaml:"access"`
		URL       string `yaml:"url"`
		IsDefault bool   `yaml:"isDefault"`
		Editable  bool   `yaml:"editable"`
	} `yaml:"datasources"`
}

// GrafanaDashboardProviderConfig represents the YAML structure of a dashboard provisioning file.
type GrafanaDashboardProviderConfig struct {
	APIVersion int `yaml:"apiVersion"`
	Providers  []struct {
		Name    string `yaml:"name"`
		OrgID   int    `yaml:"orgId"`
		Folder  string `yaml:"folder"`
		Type    string `yaml:"type"`
		Options struct {
			Path string `yaml:"path"`
		} `yaml:"options"`
	} `yaml:"providers"`
}

// DockerComposeConfig represents the structure of docker-compose.yml.
type DockerComposeConfig struct {
	Services map[string]struct {
		Image       string   `yaml:"image"`
		Container   string   `yaml:"container_name"`
		Ports       []string `yaml:"ports"`
		Volumes     []string `yaml:"volumes"`
		DependsOn   []string `yaml:"depends_on"`
		Environment []string `yaml:"environment"`
		ExtraHosts  []string `yaml:"extra_hosts"`
	} `yaml:"services"`
	Volumes map[string]struct {
		Name string `yaml:"name"`
	} `yaml:"volumes"`
}

// TestGrafanaDashboard_JSONSchemaAndMetrics validates the dashboard JSON syntax, required
// metadata, panel IDs uniqueness, and ensures all PromQL queries reference actual metrics.
func TestGrafanaDashboard_JSONSchemaAndMetrics(t *testing.T) {
	dashboardPath := filepath.Join("..", "..", "..", "infra", "prometheus", "grafana", "dashboards", "telemetry_and_recovery_operations.json")
	data, err := os.ReadFile(dashboardPath)
	if err != nil {
		t.Fatalf("failed to read dashboard JSON file at %s: %v", dashboardPath, err)
	}

	var dash GrafanaDashboard
	if err := json.Unmarshal(data, &dash); err != nil {
		t.Fatalf("JSON unmarshal failed for %s: %v", dashboardPath, err)
	}

	if dash.Title != "AegisEdge — Telemetry & Recovery Operations" {
		t.Errorf("unexpected dashboard title: %q", dash.Title)
	}
	if dash.UID != "aegisedge-telemetry-recovery" {
		t.Errorf("unexpected dashboard UID: %q", dash.UID)
	}
	if dash.SchemaVersion < 38 {
		t.Errorf("expected schemaVersion >= 38, got %d", dash.SchemaVersion)
	}
	if len(dash.Panels) < 10 {
		t.Fatalf("expected at least 10 panels, got %d", len(dash.Panels))
	}

	seenPanelIDs := make(map[int]string)
	seenMetrics := make(map[string]bool)

	// Set of legitimate, registered metrics in AegisEdge or Prometheus synthetics
	allowedMetrics := map[string]bool{
		MetricQuarantineActiveRecords:     true,
		MetricNATSPendingRecoveryItems:    true,
		MetricNATSTerminatedMessagesTotal: true,
		MetricIngestedBatchesTotal:        true,
		MetricStorageProbeFailuresTotal:   true,
		MetricQuarantineReplaysTotal:      true,
		MetricQuarantineDiscardsTotal:     true,
		"up":                              true,
		"ALERTS":                          true,
	}

	metricRegex := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)`)

	// List of known PromQL functions and keywords to ignore during identifier extraction
	promQLKeywords := map[string]bool{
		"rate": true, "sum": true, "increase": true, "by": true, "without": true,
		"job": true, "outcome": true, "alertname": true, "alertstate": true,
		"severity": true, "special": true, "table": true, "or": true,
	}

	for _, p := range dash.Panels {
		if p.ID <= 0 {
			t.Errorf("panel has non-positive ID: %d (title: %q)", p.ID, p.Title)
		}
		if origTitle, exists := seenPanelIDs[p.ID]; exists {
			t.Errorf("duplicate panel ID %d found for panels %q and %q", p.ID, origTitle, p.Title)
		}
		seenPanelIDs[p.ID] = p.Title

		if strings.TrimSpace(p.Title) == "" {
			t.Errorf("panel ID %d has empty title", p.ID)
		}

		if p.Type != "row" {
			if strings.TrimSpace(p.Description) == "" {
				t.Errorf("panel %q (ID %d) has empty description", p.Title, p.ID)
			}
			if len(p.Targets) == 0 {
				t.Errorf("non-row panel %q (ID %d) has no targets", p.Title, p.ID)
			}
		}

		for _, tgt := range p.Targets {
			expr := strings.TrimSpace(tgt.Expr)
			if expr == "" {
				t.Errorf("panel %q target has empty expression", p.Title)
				continue
			}

			tokens := metricRegex.FindAllString(expr, -1)
			for _, tok := range tokens {
				if promQLKeywords[tok] {
					continue
				}
				if strings.HasPrefix(tok, "aegisedge_") || tok == "up" || tok == "ALERTS" {
					if !allowedMetrics[tok] {
						t.Errorf("panel %q references unknown metric %q in expr: %s", p.Title, tok, expr)
					}
					seenMetrics[tok] = true
				}
			}
		}
	}

	// Verify that the critical telemetry and recovery metrics are all represented in the dashboard
	requiredMetrics := []string{
		MetricQuarantineActiveRecords,
		MetricNATSPendingRecoveryItems,
		MetricNATSTerminatedMessagesTotal,
		"up",
	}

	for _, rm := range requiredMetrics {
		if !seenMetrics[rm] {
			t.Errorf("required metric %q is not queried by any dashboard panel", rm)
		}
	}
}

// TestGrafanaProvisioning_DatasourcesAndDashboards validates datasource and dashboard provider YAML files.
func TestGrafanaProvisioning_DatasourcesAndDashboards(t *testing.T) {
	// 1. Datasource validation
	dsPath := filepath.Join("..", "..", "..", "infra", "prometheus", "grafana", "provisioning", "datasources", "prometheus.yml")
	dsData, err := os.ReadFile(dsPath)
	if err != nil {
		t.Fatalf("failed to read datasource provisioning file: %v", err)
	}

	var dsCfg GrafanaDatasourceConfig
	if err := yaml.Unmarshal(dsData, &dsCfg); err != nil {
		t.Fatalf("YAML unmarshal failed for %s: %v", dsPath, err)
	}

	if len(dsCfg.Datasources) != 1 {
		t.Fatalf("expected exactly 1 datasource, got %d", len(dsCfg.Datasources))
	}
	ds := dsCfg.Datasources[0]
	if ds.Name != "Prometheus" {
		t.Errorf("expected datasource name 'Prometheus', got %q", ds.Name)
	}
	if ds.Type != "prometheus" {
		t.Errorf("expected datasource type 'prometheus', got %q", ds.Type)
	}
	if ds.URL != "http://prometheus:9090" {
		t.Errorf("expected datasource URL 'http://prometheus:9090', got %q", ds.URL)
	}
	if !ds.IsDefault {
		t.Errorf("expected datasource to be default")
	}

	// 2. Dashboard provider validation
	dpPath := filepath.Join("..", "..", "..", "infra", "prometheus", "grafana", "provisioning", "dashboards", "dashboards.yml")
	dpData, err := os.ReadFile(dpPath)
	if err != nil {
		t.Fatalf("failed to read dashboard provider file: %v", err)
	}

	var dpCfg GrafanaDashboardProviderConfig
	if err := yaml.Unmarshal(dpData, &dpCfg); err != nil {
		t.Fatalf("YAML unmarshal failed for %s: %v", dpPath, err)
	}

	if len(dpCfg.Providers) != 1 {
		t.Fatalf("expected exactly 1 dashboard provider, got %d", len(dpCfg.Providers))
	}
	provider := dpCfg.Providers[0]
	if provider.Name != "AegisEdge" {
		t.Errorf("expected provider name 'AegisEdge', got %q", provider.Name)
	}
	if provider.Folder != "AegisEdge" {
		t.Errorf("expected folder 'AegisEdge', got %q", provider.Folder)
	}
	if provider.Options.Path != "/var/lib/grafana/dashboards" {
		t.Errorf("expected path '/var/lib/grafana/dashboards', got %q", provider.Options.Path)
	}
}

// TestGrafanaDockerCompose_Configuration validates docker-compose.yml for Grafana configuration,
// image pinning, localhost port binding, and volume path mappings.
func TestGrafanaDockerCompose_Configuration(t *testing.T) {
	composePath := filepath.Join("..", "..", "..", "infra", "prometheus", "docker-compose.yml")
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("failed to read docker-compose.yml: %v", err)
	}

	var compose DockerComposeConfig
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatalf("YAML unmarshal failed for %s: %v", composePath, err)
	}

	// Verify Prometheus service
	promService, hasProm := compose.Services["prometheus"]
	if !hasProm {
		t.Fatalf("expected 'prometheus' service in docker-compose.yml")
	}
	if promService.Image != "prom/prometheus:v3.2.1" {
		t.Errorf("expected pinned Prometheus image 'prom/prometheus:v3.2.1', got %q", promService.Image)
	}

	// Verify Grafana service
	grafanaService, hasGrafana := compose.Services["grafana"]
	if !hasGrafana {
		t.Fatalf("expected 'grafana' service in docker-compose.yml")
	}
	if grafanaService.Image != "grafana/grafana:11.5.2" {
		t.Errorf("expected pinned Grafana image 'grafana/grafana:11.5.2', got %q", grafanaService.Image)
	}

	// Verify localhost port binding for Grafana
	hasBoundPort := false
	for _, p := range grafanaService.Ports {
		if p == "127.0.0.1:3000:3000" {
			hasBoundPort = true
			break
		}
	}
	if !hasBoundPort {
		t.Errorf("expected Grafana port binding '127.0.0.1:3000:3000', got %v", grafanaService.Ports)
	}

	// Verify volume definitions
	if _, ok := compose.Volumes["prometheus_data"]; !ok {
		t.Errorf("expected volume 'prometheus_data' in compose volumes")
	}
	if _, ok := compose.Volumes["grafana_data"]; !ok {
		t.Errorf("expected volume 'grafana_data' in compose volumes")
	}

	// Verify mounted host directories exist
	baseDir := filepath.Dir(composePath)
	checkDirs := []string{
		filepath.Join(baseDir, "grafana", "provisioning", "datasources"),
		filepath.Join(baseDir, "grafana", "provisioning", "dashboards"),
		filepath.Join(baseDir, "grafana", "dashboards"),
	}
	for _, dir := range checkDirs {
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			t.Errorf("expected mounted directory %s to exist: %v", dir, err)
		}
	}
}

// TestGrafanaDashboard_StorageHealthPanel validates the PromQL expression and value mappings
// for Panel 8 (Quarantine Storage Health). It ensures finite values yield 0 (healthy),
// NaNs yield NaN (failure), and missing metrics yield null/empty (no data).
func TestGrafanaDashboard_StorageHealthPanel(t *testing.T) {
	dashboardPath := filepath.Join("..", "..", "..", "infra", "prometheus", "grafana", "dashboards", "telemetry_and_recovery_operations.json")
	data, err := os.ReadFile(dashboardPath)
	if err != nil {
		t.Fatalf("failed to read dashboard JSON file at %s: %v", dashboardPath, err)
	}

	var dash GrafanaDashboard
	if err := json.Unmarshal(data, &dash); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}

	var panel8 *GrafanaPanel
	for i := range dash.Panels {
		if dash.Panels[i].ID == 8 {
			panel8 = &dash.Panels[i]
			break
		}
	}

	if panel8 == nil {
		t.Fatalf("panel 8 not found in dashboard")
	}

	if panel8.Title != "Quarantine Storage Health (NaN Check)" {
		t.Errorf("unexpected panel 8 title: %q", panel8.Title)
	}

	expectedExpr := "(aegisedge_quarantine_active_records != aegisedge_quarantine_active_records) or (0 * aegisedge_quarantine_active_records)"
	if len(panel8.Targets) == 0 {
		t.Fatal("panel 8 has no Prometheus targets")
	}

	if got := panel8.Targets[0].Expr; got != expectedExpr {
		t.Errorf("expected target expr %q, got %q", expectedExpr, got)
	}

	// Validate mappings
	var foundZero, foundNaN, foundNull bool
	for _, m := range panel8.FieldConfig.Defaults.Mappings {
		mType, _ := m["type"].(string)
		opts, _ := m["options"].(map[string]interface{})
		if mType == "value" && opts != nil {
			if zeroOpt, ok := opts["0"].(map[string]interface{}); ok {
				if zeroOpt["text"] == "HEALTHY (0 NaN)" && zeroOpt["color"] == "green" {
					foundZero = true
				}
			}
		} else if mType == "special" && opts != nil {
			match, _ := opts["match"].(string)
			result, _ := opts["result"].(map[string]interface{})
			if match == "nan" && result != nil {
				if result["text"] == "FAILURE (NaN DETECTED)" && result["color"] == "red" {
					foundNaN = true
				}
			}
			if match == "null" && result != nil {
				if result["text"] == "NO DATA" && result["color"] == "gray" {
					foundNull = true
				}
			}
		}
	}

	if !foundZero {
		t.Errorf("missing or incorrect value mapping for '0' -> 'HEALTHY (0 NaN)' (green)")
	}
	if !foundNaN {
		t.Errorf("missing or incorrect special mapping for 'nan' -> 'FAILURE (NaN DETECTED)' (red)")
	}
	if !foundNull {
		t.Errorf("missing or incorrect special mapping for 'null' -> 'NO DATA' (gray)")
	}
}
