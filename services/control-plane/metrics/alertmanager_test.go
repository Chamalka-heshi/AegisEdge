package metrics

import (
	"encoding/json"
	"testing"
	"time"
)

// WebhookAlert represents an alert in the Alertmanager webhook payload.
type WebhookAlert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// WebhookMessage represents the Alertmanager webhook delivery payload.
type WebhookMessage struct {
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
	TruncatedAlerts   int               `json:"truncatedAlerts"`
	Status            string            `json:"status"`
	Receiver          string            `json:"receiver"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Alerts            []WebhookAlert    `json:"alerts"`
}

// TestAlertmanagerWebhook_PayloadUnmarshal validates parsing of Alertmanager webhook delivery JSON.
func TestAlertmanagerWebhook_PayloadUnmarshal(t *testing.T) {
	rawJSON := `{
		"version": "4",
		"groupKey": "{}:{alertname=\"QuarantineActiveBacklogCritical\"}",
		"truncatedAlerts": 0,
		"status": "firing",
		"receiver": "critical-receiver",
		"groupLabels": {
			"alertname": "QuarantineActiveBacklogCritical",
			"severity": "critical"
		},
		"commonLabels": {
			"alertname": "QuarantineActiveBacklogCritical",
			"severity": "critical"
		},
		"commonAnnotations": {
			"summary": "Critically high active telemetry quarantine backlog",
			"description": "Active durable quarantine backlog has reached 600 records (threshold > 500) for more than 5m."
		},
		"externalURL": "http://127.0.0.1:9093",
		"alerts": [
			{
				"status": "firing",
				"labels": {
					"alertname": "QuarantineActiveBacklogCritical",
					"severity": "critical"
				},
				"annotations": {
					"summary": "Critically high active telemetry quarantine backlog",
					"description": "Active durable quarantine backlog has reached 600 records (threshold > 500) for more than 5m."
				},
				"startsAt": "2026-11-09T05:00:00Z",
				"endsAt": "0001-01-01T00:00:00Z",
				"generatorURL": "http://127.0.0.1:9090/graph",
				"fingerprint": "a1b2c3d4e5"
			}
		]
	}`

	var msg WebhookMessage
	if err := json.Unmarshal([]byte(rawJSON), &msg); err != nil {
		t.Fatalf("failed to unmarshal Alertmanager webhook payload: %v", err)
	}

	if msg.Status != "firing" {
		t.Errorf("expected status 'firing', got %q", msg.Status)
	}
	if msg.Receiver != "critical-receiver" {
		t.Errorf("expected receiver 'critical-receiver', got %q", msg.Receiver)
	}
	if len(msg.Alerts) != 1 {
		t.Fatalf("expected 1 alert in payload, got %d", len(msg.Alerts))
	}

	alert := msg.Alerts[0]
	if alert.Status != "firing" {
		t.Errorf("expected alert status 'firing', got %q", alert.Status)
	}
	if alert.Labels["severity"] != "critical" {
		t.Errorf("expected severity 'critical', got %q", alert.Labels["severity"])
	}
	if alert.Annotations["summary"] != "Critically high active telemetry quarantine backlog" {
		t.Errorf("summary mismatch: got %q", alert.Annotations["summary"])
	}
}

// TestAlertmanagerWebhook_ResolvedPayload validates parsing of Alertmanager resolved webhook delivery JSON.
func TestAlertmanagerWebhook_ResolvedPayload(t *testing.T) {
	rawJSON := `{
		"version": "4",
		"groupKey": "{}:{alertname=\"QuarantineActiveBacklogHigh\"}",
		"status": "resolved",
		"receiver": "warning-receiver",
		"groupLabels": {
			"alertname": "QuarantineActiveBacklogHigh",
			"severity": "warning"
		},
		"alerts": [
			{
				"status": "resolved",
				"labels": {
					"alertname": "QuarantineActiveBacklogHigh",
					"severity": "warning"
				},
				"startsAt": "2026-11-09T05:00:00Z",
				"endsAt": "2026-11-09T05:10:00Z"
			}
		]
	}`

	var msg WebhookMessage
	if err := json.Unmarshal([]byte(rawJSON), &msg); err != nil {
		t.Fatalf("failed to unmarshal resolved Alertmanager webhook payload: %v", err)
	}

	if msg.Status != "resolved" {
		t.Errorf("expected status 'resolved', got %q", msg.Status)
	}
	if msg.Receiver != "warning-receiver" {
		t.Errorf("expected receiver 'warning-receiver', got %q", msg.Receiver)
	}
	if len(msg.Alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(msg.Alerts))
	}
	if msg.Alerts[0].Status != "resolved" {
		t.Errorf("expected alert status 'resolved', got %q", msg.Alerts[0].Status)
	}
	if msg.Alerts[0].EndsAt.IsZero() {
		t.Errorf("expected non-zero endsAt timestamp for resolved alert")
	}
}
