package testutil

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// GenerateTestTLSCertificate generates a self-signed ECDSA certificate and private key in the given directory.
func GenerateTestTLSCertificate(t *testing.T, dir string) (string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"AegisEdge Security Test"},
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

// BuildSignedRequest constructs an *http.Request signed with the provided HMAC secret and security headers.
func BuildSignedRequest(t *testing.T, method, targetURL, nodeID, secret string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, targetURL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	reqTarget := req.URL.RequestURI()
	if reqTarget == "" {
		reqTarget = req.URL.Path
	}

	headers, err := types.CreateAuthHeaders([]byte(secret), method, reqTarget, nodeID, body)
	if err != nil {
		t.Fatalf("failed to create auth headers: %v", err)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

// AssertNoSecretDisclosure asserts that error messages, response bodies, or logs do NOT contain the secret value.
func AssertNoSecretDisclosure(t *testing.T, secret string, text ...string) {
	t.Helper()
	trimmed := strings.TrimSpace(secret)
	if len(trimmed) < 4 {
		return // Ignore trivially short markers
	}
	for i, s := range text {
		if strings.Contains(s, trimmed) {
			t.Fatalf("SECURITY VIOLATION: secret value was disclosed in text output item %d", i)
		}
	}
}
