package opcua

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	SecurityPolicyNone = "None"
	SecurityModeNone   = "None"

	defaultRequestTimeout    = 10 * time.Second
	defaultReconnectInterval = 5 * time.Second
	defaultMaxRetryBackoff   = 30 * time.Second
)

// Config describes how to connect to an OPC-UA server.
type Config struct {
	Endpoint string // opc.tcp://host:port

	// Seguridad. Con None/None (por defecto) no se cifra ni se firma.
	SecurityPolicy string // None, Basic256Sha256, Aes128_Sha256_RsaOaep, Aes256_Sha256_RsaPss
	SecurityMode   string // None, Sign, SignAndEncrypt
	CertFile       string // certificado del cliente (PEM o DER); obligatorio si SecurityPolicy != None
	KeyFile        string // clave privada del cliente; obligatoria si SecurityPolicy != None
	ApplicationURI string // debe coincidir con el subjectAltName URI del certificado

	// Autenticación. Sin Username se conecta como anónimo.
	Username string
	Password string

	RequestTimeout    time.Duration // por defecto 10s
	ReconnectInterval time.Duration // espera entre reconexiones automáticas tras una caída; por defecto 5s
	MaxRetryBackoff   time.Duration // tope del backoff de la conexión inicial; por defecto 30s
}

// withDefaults returns a copy of the config with empty values filled in.
func (c Config) withDefaults() Config {
	if c.SecurityPolicy == "" {
		c.SecurityPolicy = SecurityPolicyNone
	}
	if c.SecurityMode == "" {
		c.SecurityMode = SecurityModeNone
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = defaultRequestTimeout
	}
	if c.ReconnectInterval <= 0 {
		c.ReconnectInterval = defaultReconnectInterval
	}
	if c.MaxRetryBackoff <= 0 {
		c.MaxRetryBackoff = defaultMaxRetryBackoff
	}
	return c
}

func (c Config) validate() error {
	if strings.TrimSpace(c.Endpoint) == "" {
		return errors.New("opcua: endpoint is required")
	}
	if !strings.HasPrefix(c.Endpoint, "opc.tcp://") {
		return fmt.Errorf("opcua: endpoint %q must start with opc.tcp://", c.Endpoint)
	}

	noPolicy := c.SecurityPolicy == SecurityPolicyNone
	noMode := c.SecurityMode == SecurityModeNone
	if noPolicy != noMode {
		return fmt.Errorf("opcua: security policy %q and mode %q are incompatible: both must be None or neither",
			c.SecurityPolicy, c.SecurityMode)
	}
	if !noPolicy && (c.CertFile == "" || c.KeyFile == "") {
		return fmt.Errorf("opcua: security policy %q requires CertFile and KeyFile", c.SecurityPolicy)
	}
	if c.Password != "" && c.Username == "" {
		return errors.New("opcua: password given without username")
	}
	return nil
}
