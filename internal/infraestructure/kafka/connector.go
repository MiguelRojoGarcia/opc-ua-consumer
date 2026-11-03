// Package kafka contains the infrastructure to talk to Kafka.
//
// Like the OPC-UA connector, Connector prepares the connection from a Config
// and hands out a ready-to-use client. The client is a franz-go type, so it
// must only be used inside the infrastructure layer (by the adapters that
// implement application ports).
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"opc-ua-consumer/internal/shared/logger"
)

// Connector creates connections to a Kafka cluster.
type Connector struct {
	cfg Config
	log logger.Logger

	opts []kgo.Opt // opciones extra; solo las usan los tests (p. ej. hooks de métricas)
}

// NewConnector validates the config and returns a Connector.
func NewConnector(cfg Config, log logger.Logger) (*Connector, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Connector{cfg: cfg, log: log}, nil
}

// Connect returns a client once the cluster answers.
//
// franz-go connects lazily, so Connect pings the brokers and retries with
// exponential backoff (1s up to MaxRetryBackoff) until one answers or ctx is
// cancelled. Messages are partitioned by key with murmur2, the same hash as the
// Java client, so a key always goes to the same partition. The producer is
// idempotent with acks=all (franz-go defaults): no duplicates nor reordering on
// retries. The caller owns the client and must release it with Close.
func (c *Connector) Connect(ctx context.Context) (*kgo.Client, error) {
	codecs, err := c.cfg.compressionCodecs()
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(c.cfg.Brokers...),
		kgo.ClientID(c.cfg.ClientID),
		kgo.ProducerLinger(c.cfg.Linger),
		kgo.ProducerBatchMaxBytes(c.cfg.BatchMaxBytes),
		kgo.ProducerBatchCompression(codecs...),
		kgo.MaxBufferedRecords(c.cfg.MaxBufferedRecords),
		kgo.MaxBufferedBytes(c.cfg.MaxBufferedBytes),
		kgo.RecordDeliveryTimeout(c.cfg.DeliveryTimeout),
	}, c.opts...)...)
	if err != nil {
		return nil, err
	}

	backoff := time.Second
	for {
		err := client.Ping(ctx)
		if err == nil {
			c.log.Info(ctx, "connected to Kafka",
				"brokers", c.cfg.Brokers, "linger", c.cfg.Linger, "batch_max_bytes", c.cfg.BatchMaxBytes,
				"compression", c.cfg.Compression, "max_buffered_records", c.cfg.MaxBufferedRecords,
				"max_buffered_bytes", c.cfg.MaxBufferedBytes)
			return client, nil
		}
		c.log.Warn(ctx, "could not connect to Kafka, retrying",
			"brokers", c.cfg.Brokers, "error", err, "retry_in", backoff)

		select {
		case <-ctx.Done():
			client.Close()
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, c.cfg.MaxRetryBackoff)
	}
}

// Close waits until the buffered records are delivered (or ctx expires) and
// closes the client. Records still pending when ctx expires are lost.
func Close(ctx context.Context, client *kgo.Client) error {
	defer client.Close()
	if err := client.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: flushing buffer: %w (%d records left)", err, client.BufferedProduceRecords())
	}
	return nil
}
