package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func TestProduce(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "telemetry"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	brokers := strings.Join(cluster.ListenAddrs(), ",")

	cfg, err := config.Parse([]byte(`inputs: [{type: x}]
outputs: [{type: kafka, brokers: [` + brokers + `], topic: telemetry, compression: zstd}]`))
	if err != nil {
		t.Fatal(err)
	}
	o, err := New(&cfg.Outputs[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	m := telemetry.New("/interfaces/interface", time.Unix(1700000000, 0).UTC())
	m.Tags = map[string]string{"source": "r1", "interface_name": "Eth1"}
	m.Fields = map[string]any{"in-octets": uint64(42), "oper-status": "UP"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := o.Write(ctx, []*telemetry.Metric{m}); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(ctx); err != nil {
		t.Fatal(err)
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics("telemetry"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	fetches := consumer.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatal(errs)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("got %d records", len(recs))
	}
	if string(recs[0].Key) != "r1" {
		t.Errorf("key %q", recs[0].Key)
	}
	var got map[string]any
	if err := json.Unmarshal(recs[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got["name"] != "/interfaces/interface" || got["fields"].(map[string]any)["in-octets"] != 42.0 ||
		got["timestamp"] != "2023-11-14T22:13:20Z" {
		t.Errorf("record %s", recs[0].Value)
	}
}

func TestConfigErrors(t *testing.T) {
	for _, y := range []string{
		`{type: kafka, topic: t}`,
		`{type: kafka, brokers: [a:1], topic: t, compression: brotli}`,
		`{type: kafka, brokers: [a:1], topic: t, acks: some}`,
		`{type: kafka, brokers: [a:1], topic: t, sasl: {mechanism: GSSAPI}}`,
	} {
		cfg, _ := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - " + y))
		if _, err := New(&cfg.Outputs[0], pipeline.Env{Logger: slog.Default()}); err == nil {
			t.Errorf("expected error for %s", y)
		}
	}
}
