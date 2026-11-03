package opcua

import (
	"testing"
	"time"

	"opc-ua-consumer/internal/shared/logger"
)

func TestConfigDefaults(t *testing.T) {
	cfg := Config{Endpoint: "opc.tcp://localhost:49320"}.withDefaults()
	if cfg.SecurityPolicy != SecurityPolicyNone || cfg.SecurityMode != SecurityModeNone {
		t.Errorf("security defaults: %q %q", cfg.SecurityPolicy, cfg.SecurityMode)
	}
	if cfg.RequestTimeout != 10*time.Second || cfg.ReconnectInterval != 5*time.Second || cfg.MaxRetryBackoff != 30*time.Second {
		t.Errorf("timing defaults: %+v", cfg)
	}
}

func TestNewConnectorValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"minimal", Config{Endpoint: "opc.tcp://localhost:49320"}, false},
		{"empty endpoint", Config{}, true},
		{"wrong scheme", Config{Endpoint: "http://localhost:49320"}, true},
		{"policy without mode", Config{Endpoint: "opc.tcp://h:1", SecurityPolicy: "Basic256Sha256"}, true},
		{"mode without policy", Config{Endpoint: "opc.tcp://h:1", SecurityMode: "SignAndEncrypt"}, true},
		{"security without cert", Config{Endpoint: "opc.tcp://h:1", SecurityPolicy: "Basic256Sha256", SecurityMode: "SignAndEncrypt"}, true},
		{"security with cert", Config{Endpoint: "opc.tcp://h:1", SecurityPolicy: "Basic256Sha256", SecurityMode: "SignAndEncrypt",
			CertFile: "c.crt", KeyFile: "c.key"}, false},
		{"password without user", Config{Endpoint: "opc.tcp://h:1", Password: "x"}, true},
		{"user and password", Config{Endpoint: "opc.tcp://h:1", Username: "u", Password: "x"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConnector(tt.cfg, logger.Nop())
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
