package receiver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"

	"github.com/pion/dtls/v3"
)

// TLSMode selects the DTLS authentication method for AgenticUDP.
type TLSMode string

const (
	TLSModeNone TLSMode = "none" // cleartext UDP (default, dev only)
	TLSModePSK  TLSMode = "psk"  // pre-shared key derived from tenant token
	TLSModeCert TLSMode = "cert" // mutual TLS with X.509 certificates
)

// DTLSConfig holds the DTLS parameters for the AgenticUDP receiver.
type DTLSConfig struct {
	Mode     TLSMode
	CertFile string // PEM certificate (TLSModeCert)
	KeyFile  string // PEM private key (TLSModeCert)

	// PSKCallback returns the pre-shared key for a given identity hint.
	// For TLSModePSK, this is typically derived from the tenant token.
	PSKCallback func(hint []byte) ([]byte, error)
}

// dtlsListener wraps a raw UDP connection with a DTLS server listener.
// Each accepted DTLS connection is a secure, multiplexed session.
type dtlsListener struct {
	listener net.Listener
	cancel   context.CancelFunc
}

// newDTLSListener creates a DTLS listener on the given UDP address.
// Returns the listener and its underlying UDP address for logging.
func newDTLSListener(addr string, cfg DTLSConfig) (*dtlsListener, net.Addr, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("dtls: resolve %s: %w", addr, err)
	}

	dtlsCfg, err := buildServerDTLSConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("dtls: config: %w", err)
	}

	listener, err := dtls.Listen("udp", udpAddr, dtlsCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("dtls: listen %s: %w", addr, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	_ = ctx

	log.Printf("agenticudp: DTLS %s listener on %s", cfg.Mode, listener.Addr())
	return &dtlsListener{listener: listener, cancel: cancel}, listener.Addr(), nil
}

func (l *dtlsListener) Accept() (net.Conn, error) {
	return l.listener.Accept()
}

func (l *dtlsListener) Close() error {
	l.cancel()
	return l.listener.Close()
}

func buildServerDTLSConfig(cfg DTLSConfig) (*dtls.Config, error) {
	switch cfg.Mode {
	case TLSModePSK:
		if cfg.PSKCallback == nil {
			return nil, fmt.Errorf("PSK mode requires PSKCallback")
		}
		return &dtls.Config{
			PSK: cfg.PSKCallback,
			PSKIdentityHint: []byte("entropyops"),
			CipherSuites: []dtls.CipherSuiteID{
				dtls.TLS_PSK_WITH_AES_128_GCM_SHA256,
			},
		}, nil

	case TLSModeCert:
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load cert/key: %w", err)
		}
		certPool := x509.NewCertPool()
		return &dtls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   dtls.RequireAnyClientCert,
			RootCAs:      certPool,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported TLS mode %q for DTLS listener", cfg.Mode)
	}
}
