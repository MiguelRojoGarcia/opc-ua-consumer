// Package opcua contains the infrastructure to talk to OPC-UA servers.
//
// Connector plays the same role as a database connector: it prepares the
// connection from a Config and hands out a ready-to-use client. The client is a
// gopcua type, so it must only be used inside the infrastructure layer (by the
// adapters that implement application ports), never by the domain or the
// application layer.
package opcua

import (
	"context"
	"fmt"
	"time"

	gopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"

	"opc-ua-consumer/internal/shared/logger"
)

// Connector creates connections to an OPC-UA server.
type Connector struct {
	cfg Config
	log logger.Logger
}

// NewConnector validates the config and returns a Connector.
func NewConnector(cfg Config, log logger.Logger) (*Connector, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Connector{cfg: cfg, log: log}, nil
}

// Connect opens a session with the server and returns the connected client.
//
// If the server is not reachable it retries with exponential backoff (1s up to
// MaxRetryBackoff) until it connects or ctx is cancelled. Once connected,
// drops are handled by gopcua itself (AutoReconnect), which also restores the
// subscriptions. The caller owns the client and must Close it.
func (c *Connector) Connect(ctx context.Context) (*gopcua.Client, error) {
	backoff := time.Second
	for {
		client, err := c.dial(ctx)
		if err == nil {
			c.log.Info(ctx, "connected to OPC-UA server",
				"endpoint", c.cfg.Endpoint, "security_policy", c.cfg.SecurityPolicy, "security_mode", c.cfg.SecurityMode)
			return client, nil
		}
		c.log.Warn(ctx, "could not connect to OPC-UA server, retrying",
			"endpoint", c.cfg.Endpoint, "error", err, "retry_in", backoff)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, c.cfg.MaxRetryBackoff)
	}
}

// dial makes a single connection attempt.
func (c *Connector) dial(ctx context.Context) (*gopcua.Client, error) {
	endpoints, err := gopcua.GetEndpoints(ctx, c.cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("getting endpoints: %w", err)
	}
	ep, err := gopcua.SelectEndpoint(endpoints, c.cfg.SecurityPolicy, ua.MessageSecurityModeFromString(c.cfg.SecurityMode))
	if err != nil {
		return nil, fmt.Errorf("selecting endpoint: %w", err)
	}

	opts := []gopcua.Option{
		gopcua.AutoReconnect(true),
		gopcua.ReconnectInterval(c.cfg.ReconnectInterval),
		gopcua.RequestTimeout(c.cfg.RequestTimeout),
	}
	if c.cfg.CertFile != "" {
		opts = append(opts, gopcua.CertificateFile(c.cfg.CertFile), gopcua.PrivateKeyFile(c.cfg.KeyFile))
	}
	if c.cfg.ApplicationURI != "" {
		opts = append(opts, gopcua.ApplicationURI(c.cfg.ApplicationURI))
	}
	authType := ua.UserTokenTypeAnonymous
	if c.cfg.Username != "" {
		authType = ua.UserTokenTypeUserName
		opts = append(opts, gopcua.AuthUsername(c.cfg.Username, c.cfg.Password))
	} else {
		opts = append(opts, gopcua.AuthAnonymous())
	}
	opts = append(opts, gopcua.SecurityFromEndpoint(ep, authType))

	// Se usa la URL configurada y no ep.EndpointURL, que puede traer un
	// hostname interno del servidor no resoluble desde el cliente.
	client, err := gopcua.NewClient(c.cfg.Endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}
	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	return client, nil
}
