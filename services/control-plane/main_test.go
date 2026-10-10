package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestServerConfig_Defaults(t *testing.T) {
	cfg := ServerConfig{
		Port:     8080,
		LogLevel: "info",
	}

	if cfg.Port != 8080 {
		t.Errorf("expected Port 8080, got %d", cfg.Port)
	}

	if cfg.LogLevel != "info" {
		t.Errorf("expected LogLevel 'info', got %q", cfg.LogLevel)
	}

	if Version != "0.3.0-dev" {
		t.Errorf("expected Version '0.3.0-dev', got %q", Version)
	}
	if AppName != "aegisedge-control-plane" {
		t.Errorf("expected AppName 'aegisedge-control-plane', got %q", AppName)
	}
}

func generateMainTestCertificate(t *testing.T, dir string) (string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"AegisEdge Test"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPath := filepath.Join(dir, "cert.pem")
	certOut, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("failed to create cert file: %v", err)
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		t.Fatalf("failed to encode cert PEM: %v", err)
	}

	keyPath := filepath.Join(dir, "key.pem")
	keyOut, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("failed to create key file: %v", err)
	}
	defer keyOut.Close()
	privBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to marshal private key: %v", err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privBytes}); err != nil {
		t.Fatalf("failed to encode key PEM: %v", err)
	}

	return certPath, keyPath
}

func TestBuildServerConfig_ProductionStartupWiring(t *testing.T) {
	tempDir := t.TempDir()
	certFile, keyFile := generateMainTestCertificate(t, tempDir)

	secretsFile := filepath.Join(tempDir, "node-secrets.json")
	content := `{
		"node-alpha": "alpha-secret-key-16chars",
		"node-bravo": "bravo-secret-key-16chars"
	}`
	if err := os.WriteFile(secretsFile, []byte(content), 0600); err != nil {
		t.Fatalf("failed to write secrets file: %v", err)
	}

	// 1. Valid production startup with NodeSecretsFile
	rawFlags := ServerConfig{
		Host:            "0.0.0.0",
		Port:            8443,
		AuthEnabled:     true,
		NodeSecretsFile: secretsFile,
		AdminToken:      "admin-token-super-secure-16chars",
		TLSCertFile:     certFile,
		TLSKeyFile:      keyFile,
	}

	cfg, authCfg, err := BuildServerConfig(rawFlags, true)
	if err != nil {
		t.Fatalf("expected BuildServerConfig to succeed in production, got: %v", err)
	}
	if len(authCfg.NodeSecrets) != 2 {
		t.Errorf("expected 2 node secrets in authConfig, got %d", len(authCfg.NodeSecrets))
	}
	if authCfg.NodeSecrets["node-alpha"] != "alpha-secret-key-16chars" {
		t.Errorf("unexpected secret for node-alpha")
	}

	// 2. Production startup fails if NodeSecretsFile is missing
	rawFlagsNoSecrets := rawFlags
	rawFlagsNoSecrets.NodeSecretsFile = ""
	_, _, err = BuildServerConfig(rawFlagsNoSecrets, true)
	if err == nil {
		t.Fatalf("expected error when NodeSecretsFile is empty in production, got nil")
	}

	// 3. Production startup fails if SharedSecret is also provided
	rawFlagsWithShared := rawFlags
	rawFlagsWithShared.SharedSecret = "shared-secret-key-16chars"
	_, _, err = BuildServerConfig(rawFlagsWithShared, true)
	if err == nil {
		t.Fatalf("expected error when SharedSecret is provided in production, got nil")
	}

	// 4. Production startup fails if NodeSecretsFile points to nonexistent file
	rawFlagsInvalidFile := rawFlags
	rawFlagsInvalidFile.NodeSecretsFile = filepath.Join(tempDir, "nonexistent.json")
	_, _, err = BuildServerConfig(rawFlagsInvalidFile, true)
	if err == nil {
		t.Fatalf("expected error when NodeSecretsFile does not exist, got nil")
	}

	_ = cfg
}

func TestBuildServerConfig_PrecedenceAndEnvOverrides(t *testing.T) {
	tempDir := t.TempDir()
	secretsFile := filepath.Join(tempDir, "env-secrets.json")
	content := `{"node-env": "env-secret-key-16chars"}`
	if err := os.WriteFile(secretsFile, []byte(content), 0600); err != nil {
		t.Fatalf("failed to write secrets file: %v", err)
	}

	cliSecretsFile := filepath.Join(tempDir, "cli-secrets.json")
	contentCLI := `{"node-cli": "cli-secret-key-16chars"}`
	if err := os.WriteFile(cliSecretsFile, []byte(contentCLI), 0600); err != nil {
		t.Fatalf("failed to write secrets file: %v", err)
	}

	// Set environment variables
	t.Setenv("NODE_SECRETS_FILE", secretsFile)
	t.Setenv("ADMIN_TOKEN", "env-admin-token-16chars")

	// Case A: CLI flag empty -> falls back to env variable
	rawFlagsA := ServerConfig{
		Host: "127.0.0.1",
		Port: 8080,
	}
	cfgA, authCfgA, err := BuildServerConfig(rawFlagsA, false)
	if err != nil {
		t.Fatalf("expected success with env fallback, got: %v", err)
	}
	if cfgA.NodeSecretsFile != secretsFile {
		t.Errorf("expected cfg.NodeSecretsFile %q, got %q", secretsFile, cfgA.NodeSecretsFile)
	}
	if authCfgA.NodeSecrets["node-env"] != "env-secret-key-16chars" {
		t.Errorf("expected node-env secret loaded from env-specified file")
	}

	// Case B: CLI flag explicitly provided -> overrides environment variable
	rawFlagsB := ServerConfig{
		Host:            "127.0.0.1",
		Port:            8080,
		NodeSecretsFile: cliSecretsFile,
		AdminToken:      "cli-admin-token-16chars",
	}
	cfgB, authCfgB, err := BuildServerConfig(rawFlagsB, false)
	if err != nil {
		t.Fatalf("expected success with CLI overrides, got: %v", err)
	}
	if cfgB.NodeSecretsFile != cliSecretsFile {
		t.Errorf("expected CLI flag to override env variable, got %q", cfgB.NodeSecretsFile)
	}
	if authCfgB.NodeSecrets["node-cli"] != "cli-secret-key-16chars" {
		t.Errorf("expected node-cli secret loaded from CLI-specified file")
	}
	if authCfgB.AdminToken != "cli-admin-token-16chars" {
		t.Errorf("expected CLI admin token to take precedence")
	}
}
