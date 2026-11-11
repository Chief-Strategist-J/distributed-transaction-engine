package messaging

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	MechanismPlain      = "PLAIN"
	MechanismScram256   = "SCRAM-SHA-256"
	MechanismScram512   = "SCRAM-SHA-512"
	securityDialTimeout = 10 * time.Second
)

type Security struct {
	TLSEnabled bool
	CAFile     string
	Mechanism  string
	Username   string
	Password   string
}

func (s Security) Enabled() bool {
	return s.TLSEnabled || s.Mechanism != ""
}

func (s Security) TLSConfig() (*tls.Config, error) {
	if !s.TLSEnabled {
		return nil, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if strings.TrimSpace(s.CAFile) == "" {
		return config, nil
	}
	pem, err := os.ReadFile(s.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read kafka CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("kafka CA file contains no valid certificates")
	}
	config.RootCAs = pool
	return config, nil
}

func (s Security) SASL() (sasl.Mechanism, error) {
	switch strings.ToUpper(strings.TrimSpace(s.Mechanism)) {
	case "":
		return nil, nil
	case MechanismPlain:
		return plain.Mechanism{Username: s.Username, Password: s.Password}, nil
	case MechanismScram256:
		return scram.Mechanism(scram.SHA256, s.Username, s.Password)
	case MechanismScram512:
		return scram.Mechanism(scram.SHA512, s.Username, s.Password)
	default:
		return nil, fmt.Errorf("unsupported kafka SASL mechanism %q", s.Mechanism)
	}
}

func (s Security) Dialer() (*kafka.Dialer, error) {
	tlsConfig, err := s.TLSConfig()
	if err != nil {
		return nil, err
	}
	mechanism, err := s.SASL()
	if err != nil {
		return nil, err
	}
	return &kafka.Dialer{Timeout: securityDialTimeout, TLS: tlsConfig, SASLMechanism: mechanism}, nil
}

func (s Security) Transport() (*kafka.Transport, error) {
	tlsConfig, err := s.TLSConfig()
	if err != nil {
		return nil, err
	}
	mechanism, err := s.SASL()
	if err != nil {
		return nil, err
	}
	return &kafka.Transport{TLS: tlsConfig, SASL: mechanism, DialTimeout: securityDialTimeout}, nil
}
