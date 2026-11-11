package messaging

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
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

type testAuthority struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	pemBytes    []byte
}

func newAuthority(t *testing.T, name string) testAuthority {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, _ := x509.ParseCertificate(der)
	return testAuthority{certificate: certificate, key: key, pemBytes: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (a testAuthority) serverCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano() + 1), Subject: pkix.Name{CommonName: "kafka.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.certificate, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func writePEM(t *testing.T, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func startTLSServer(t *testing.T, certificate tls.Certificate) string {
	t.Helper()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() { _ = connection.(*tls.Conn).Handshake(); _ = connection.Close() }()
		}
	}()
	return listener.Addr().String()
}

func TestSecurity_DialerTrustsOnlyTheConfiguredCertificateAuthority(t *testing.T) {
	trusted, other := newAuthority(t, "trusted"), newAuthority(t, "other")
	address := startTLSServer(t, trusted.serverCertificate(t))
	cases := []struct {
		name     string
		security Security
		wantErr  bool
	}{
		{"trusted_ca", Security{TLSEnabled: true, CAFile: writePEM(t, trusted.pemBytes)}, false},
		{"unrelated_ca", Security{TLSEnabled: true, CAFile: writePEM(t, other.pemBytes)}, true},
		{"system_roots_reject_private_ca", Security{TLSEnabled: true}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dialer, err := tc.security.Dialer()
			if err != nil {
				t.Fatal(err)
			}
			connection, err := dialer.DialContext(context.Background(), "tcp", address)
			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v got %v", tc.wantErr, err)
			}
			if connection != nil {
				_ = connection.Close()
			}
		})
	}
}

func TestSecurity_TLSConfigDefaults(t *testing.T) {
	disabled, err := Security{}.TLSConfig()
	if err != nil || disabled != nil {
		t.Fatalf("TLS must be off unless requested: %v %v", disabled, err)
	}
	enabled, err := Security{TLSEnabled: true}.TLSConfig()
	if err != nil || enabled.MinVersion != tls.VersionTLS12 {
		t.Fatalf("TLS 1.2 is the floor: %v %v", enabled, err)
	}
	for name, security := range map[string]Security{
		"missing_file": {TLSEnabled: true, CAFile: filepath.Join(t.TempDir(), "absent.pem")},
		"garbage_file": {TLSEnabled: true, CAFile: writePEM(t, []byte("not a certificate"))},
	} {
		if _, err = security.TLSConfig(); err == nil {
			t.Fatalf("%s must be rejected", name)
		}
	}
}

func TestSecurity_SASLMechanisms(t *testing.T) {
	cases := map[string]bool{"": true, "PLAIN": true, "plain": true, "SCRAM-SHA-256": true, "SCRAM-SHA-512": true, "GSSAPI": false, "OAUTHBEARER": false}
	for mechanism, valid := range cases {
		got, err := Security{Mechanism: mechanism, Username: "u", Password: "p"}.SASL()
		if (err == nil) != valid {
			t.Fatalf("mechanism %q valid=%v got err=%v", mechanism, valid, err)
		}
		if valid && mechanism != "" && got == nil {
			t.Fatalf("mechanism %q must produce a SASL mechanism", mechanism)
		}
	}
	if (Security{}).Enabled() || !(Security{TLSEnabled: true}).Enabled() || !(Security{Mechanism: "PLAIN"}).Enabled() {
		t.Fatal("Enabled must reflect TLS or SASL")
	}
}

func TestSecurity_TransportCarriesTLSAndSASL(t *testing.T) {
	authority := newAuthority(t, "ca")
	transport, err := Security{TLSEnabled: true, CAFile: writePEM(t, authority.pemBytes), Mechanism: "SCRAM-SHA-512", Username: "u", Password: "p"}.Transport()
	if err != nil || transport.TLS == nil || transport.SASL == nil {
		t.Fatalf("transport must carry both TLS and SASL: %+v %v", transport, err)
	}
	if _, err = (Security{Mechanism: "BOGUS"}).Transport(); err == nil {
		t.Fatal("an unsupported mechanism must fail transport construction")
	}
}
