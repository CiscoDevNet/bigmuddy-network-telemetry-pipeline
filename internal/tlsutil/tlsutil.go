// Package tlsutil builds crypto/tls configurations from a common YAML shape
// shared by every network-facing plugin.
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// Config is the TLS block accepted by inputs and outputs.
type Config struct {
	Enabled            bool   `yaml:"enabled"`
	CAFile             string `yaml:"ca_file"`   // client: verify server; server: verify clients (mTLS)
	CertFile           string `yaml:"cert_file"` // client cert (mTLS) or server cert
	KeyFile            string `yaml:"key_file"`
	ServerName         string `yaml:"server_name"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// Client returns a client-side tls.Config, or nil when TLS is disabled.
func (c Config) Client() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.ServerName,
		InsecureSkipVerify: c.InsecureSkipVerify} //nolint:gosec // explicit opt-in for lab devices
	if c.CAFile != "" {
		pool, err := loadPool(c.CAFile)
		if err != nil {
			return nil, err
		}
		tc.RootCAs = pool
	}
	if c.CertFile != "" || c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

// Server returns a server-side tls.Config, or nil when TLS is disabled. When
// CAFile is set, client certificates are required and verified (mTLS).
func (c Config) Server() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	if c.CertFile == "" || c.KeyFile == "" {
		return nil, errors.New("tls: cert_file and key_file are required for a server")
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	if c.CAFile != "" {
		pool, err := loadPool(c.CAFile)
		if err != nil {
			return nil, err
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tc, nil
}

func loadPool(file string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates found in %s", file)
	}
	return pool, nil
}
