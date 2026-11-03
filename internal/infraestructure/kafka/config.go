package kafka

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	CompressionZstd   = "zstd"
	CompressionLz4    = "lz4"
	CompressionSnappy = "snappy"
	CompressionGzip   = "gzip"
	CompressionNone   = "none"

	defaultClientID           = "opc-ua-consumer"
	defaultLinger             = 50 * time.Millisecond
	defaultBatchMaxBytes      = 1_000_012 // = max.message.bytes por defecto del broker
	defaultMaxBufferedRecords = 500_000
	defaultMaxBufferedBytes   = 256 << 20 // 256 MiB
	defaultCompression        = CompressionZstd
	defaultDeliveryTimeout    = 2 * time.Minute
	defaultMaxRetryBackoff    = 30 * time.Second
)

// Config describes how to connect to a Kafka cluster and how to batch the
// produced records. The defaults are tuned for a high volume of small
// messages (metrics): see the README for the reasoning.
type Config struct {
	Brokers  []string // host:port de uno o varios brokers del clúster
	ClientID string   // por defecto opc-ua-consumer

	// Batching. Un batch por partición se envía cuando llega a BatchMaxBytes o
	// cuando pasa Linger desde su primer mensaje, lo que ocurra antes.
	Linger        time.Duration // por defecto 50ms
	BatchMaxBytes int32         // tamaño máximo (sin comprimir) de un batch; no puede superar max.message.bytes del tópico; por defecto ~1MB
	Compression   string        // zstd (por defecto), lz4, snappy, gzip, none; se aplica por batch

	// Buffer en memoria de mensajes pendientes de confirmar. Cuando se llena,
	// Publish se bloquea (backpressure) hasta que haya hueco.
	MaxBufferedRecords int // por defecto 500.000
	MaxBufferedBytes   int // por defecto 256 MiB

	DeliveryTimeout time.Duration // tiempo máximo para entregar un mensaje (incluidos reintentos); por defecto 2m
	MaxRetryBackoff time.Duration // tope del backoff de la conexión inicial; por defecto 30s
}

// withDefaults returns a copy of the config with empty values filled in.
func (c Config) withDefaults() Config {
	brokers := make([]string, 0, len(c.Brokers))
	for _, b := range c.Brokers {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	c.Brokers = brokers
	if c.ClientID == "" {
		c.ClientID = defaultClientID
	}
	if c.Linger <= 0 {
		c.Linger = defaultLinger
	}
	if c.BatchMaxBytes <= 0 {
		c.BatchMaxBytes = defaultBatchMaxBytes
	}
	if c.Compression == "" {
		c.Compression = defaultCompression
	}
	c.Compression = strings.ToLower(c.Compression)
	if c.MaxBufferedRecords <= 0 {
		c.MaxBufferedRecords = defaultMaxBufferedRecords
	}
	if c.MaxBufferedBytes <= 0 {
		c.MaxBufferedBytes = defaultMaxBufferedBytes
	}
	if c.DeliveryTimeout <= 0 {
		c.DeliveryTimeout = defaultDeliveryTimeout
	}
	if c.MaxRetryBackoff <= 0 {
		c.MaxRetryBackoff = defaultMaxRetryBackoff
	}
	return c
}

func (c Config) validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("kafka: at least one broker is required")
	}
	if _, err := c.compressionCodecs(); err != nil {
		return err
	}
	if c.MaxBufferedBytes < int(c.BatchMaxBytes) {
		return fmt.Errorf("kafka: MaxBufferedBytes (%d) must be at least BatchMaxBytes (%d)", c.MaxBufferedBytes, c.BatchMaxBytes)
	}
	return nil
}

// compressionCodecs returns the codec preference: the chosen one, falling back
// to no compression if the broker does not support it.
func (c Config) compressionCodecs() ([]kgo.CompressionCodec, error) {
	switch c.Compression {
	case CompressionZstd:
		return []kgo.CompressionCodec{kgo.ZstdCompression(), kgo.NoCompression()}, nil
	case CompressionLz4:
		return []kgo.CompressionCodec{kgo.Lz4Compression(), kgo.NoCompression()}, nil
	case CompressionSnappy:
		return []kgo.CompressionCodec{kgo.SnappyCompression(), kgo.NoCompression()}, nil
	case CompressionGzip:
		return []kgo.CompressionCodec{kgo.GzipCompression(), kgo.NoCompression()}, nil
	case CompressionNone:
		return []kgo.CompressionCodec{kgo.NoCompression()}, nil
	}
	return nil, fmt.Errorf("kafka: compression %q not supported: use zstd, lz4, snappy, gzip or none", c.Compression)
}
