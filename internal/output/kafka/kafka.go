// Package kafka publishes metrics to Apache Kafka (or Redpanda, MSK,
// Confluent Cloud, Event Hubs) as JSON records, one per Metric, keyed by
// device so a device's data stays ordered within a partition.
//
// It uses franz-go, replacing Shopify/sarama + the deprecated
// bsm/sarama-cluster used in 2016, and adds idempotent production, zstd
// compression, SASL/SCRAM and TLS.
package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

func init() { pipeline.RegisterOutput("kafka", New) }

// SASL settings.
type SASL struct {
	Mechanism    string `yaml:"mechanism"` // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	PasswordFile string `yaml:"password_file"`
}

// Config is the kafka output's YAML.
type Config struct {
	Brokers     []string       `yaml:"brokers"`
	Topic       string         `yaml:"topic"`
	Key         string         `yaml:"key"`         // source (default), name, none
	Compression string         `yaml:"compression"` // zstd (default), lz4, snappy, gzip, none
	Acks        string         `yaml:"acks"`        // all (default), leader, none
	ClientID    string         `yaml:"client_id"`
	SASL        *SASL          `yaml:"sasl"`
	TLS         tlsutil.Config `yaml:"tls"`
}

// Record is the JSON document written per metric.
type Record struct {
	Name      string            `json:"name"`
	Tags      map[string]string `json:"tags"`
	Fields    map[string]any    `json:"fields"`
	Timestamp time.Time         `json:"timestamp"`
}

// Output is the Kafka producer.
type Output struct {
	cfg    Config
	client *kgo.Client
	log    *slog.Logger
}

// New builds the producer. Brokers are contacted lazily.
func New(p *config.Plugin, env pipeline.Env) (pipeline.Output, error) {
	c := Config{Key: "source", Compression: "zstd", Acks: "all", ClientID: "telemetry-pipeline"}
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	if len(c.Brokers) == 0 || c.Topic == "" {
		return nil, errors.New("brokers and topic are required")
	}
	if c.Key != "source" && c.Key != "name" && c.Key != "none" {
		return nil, fmt.Errorf("key must be source, name or none")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.DefaultProduceTopic(c.Topic),
		kgo.ClientID(c.ClientID),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProducerBatchMaxBytes(1 << 20),
	}
	switch strings.ToLower(c.Compression) {
	case "zstd":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.NoCompression()))
	case "lz4":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression(), kgo.NoCompression()))
	case "snappy":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression(), kgo.NoCompression()))
	case "gzip":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression(), kgo.NoCompression()))
	case "none":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	default:
		return nil, fmt.Errorf("unsupported compression %q", c.Compression)
	}
	switch c.Acks {
	case "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "leader":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	case "none":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
	default:
		return nil, fmt.Errorf("acks must be all, leader or none")
	}
	tc, err := c.TLS.Client()
	if err != nil {
		return nil, err
	}
	if tc != nil {
		opts = append(opts, kgo.Dialer((&tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: tc}).DialContext))
	}
	if c.SASL != nil {
		pw, err := config.Secret(c.SASL.Password, c.SASL.PasswordFile)
		if err != nil {
			return nil, fmt.Errorf("sasl password: %w", err)
		}
		switch strings.ToUpper(c.SASL.Mechanism) {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{User: c.SASL.Username, Pass: pw}.AsMechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: c.SASL.Username, Pass: pw}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: c.SASL.Username, Pass: pw}.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism %q", c.SASL.Mechanism)
		}
		if tc == nil {
			env.Logger.Warn("SASL without TLS sends credentials in clear text")
		}
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Output{cfg: c, client: client, log: env.Logger}, nil
}

// Write produces a batch and waits for acknowledgement.
func (o *Output) Write(ctx context.Context, batch []*telemetry.Metric) error {
	recs := make([]*kgo.Record, 0, len(batch))
	for _, m := range batch {
		val, err := json.Marshal(Record{Name: m.Name, Tags: m.Tags, Fields: m.Fields, Timestamp: m.Timestamp})
		if err != nil {
			continue
		}
		r := &kgo.Record{Value: val, Timestamp: m.Timestamp}
		switch o.cfg.Key {
		case "source":
			r.Key = []byte(m.Source())
		case "name":
			r.Key = []byte(m.Name)
		}
		recs = append(recs, r)
	}
	return o.client.ProduceSync(ctx, recs...).FirstErr()
}

// Close flushes and closes the client.
func (o *Output) Close(ctx context.Context) error {
	err := o.client.Flush(ctx)
	o.client.Close()
	return err
}
