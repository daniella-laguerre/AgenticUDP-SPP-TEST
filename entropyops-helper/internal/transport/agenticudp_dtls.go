package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/pion/dtls/v3"
)

// DTLSMode selects the DTLS authentication method for the AgenticUDP client.
type DTLSMode string

const (
	DTLSModeNone DTLSMode = "none" // cleartext UDP (default, dev)
	DTLSModePSK  DTLSMode = "psk"  // pre-shared key (derived from tenant token)
	DTLSModeCert DTLSMode = "cert" // mutual TLS with X.509 certificates
)

// DTLSClientConfig holds the DTLS parameters for the AgenticUDP client.
type DTLSClientConfig struct {
	Mode     DTLSMode
	PSK      []byte // raw pre-shared key bytes (TLSModePSK)
	CertFile string // PEM certificate (TLSModeCert)
	KeyFile  string // PEM private key (TLSModeCert)
	Insecure bool   // skip server certificate verification (dev only)
}

// dialDTLS wraps a raw UDP connection with a DTLS client session.
// Returns a net.Conn that transparently encrypts/decrypts all I/O.
func dialDTLS(ctx context.Context, addr *net.UDPAddr, cfg DTLSClientConfig) (net.Conn, error) {
	dtlsCfg, err := buildClientDTLSConfig(cfg)
	if err != nil {
		return nil, err
	}

	conn, err := dtls.Dial("udp", addr, dtlsCfg)
	if err != nil {
		return nil, fmt.Errorf("dtls: dial %s: %w", addr, err)
	}
	return conn, nil
}

func buildClientDTLSConfig(cfg DTLSClientConfig) (*dtls.Config, error) {
	switch cfg.Mode {
	case DTLSModePSK:
		if len(cfg.PSK) == 0 {
			return nil, fmt.Errorf("PSK mode requires non-empty key")
		}
		psk := make([]byte, len(cfg.PSK))
		copy(psk, cfg.PSK)
		return &dtls.Config{
			PSK: func(hint []byte) ([]byte, error) {
				return psk, nil
			},
			PSKIdentityHint: []byte("entropyops"),
			CipherSuites: []dtls.CipherSuiteID{
				dtls.TLS_PSK_WITH_AES_128_GCM_SHA256,
			},
		}, nil

	case DTLSModeCert:
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("dtls: load cert/key: %w", err)
		}
		return &dtls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: cfg.Insecure,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported DTLS mode %q", cfg.Mode)
	}
}
