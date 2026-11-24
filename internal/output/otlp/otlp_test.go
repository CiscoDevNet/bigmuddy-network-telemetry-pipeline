package otlp

import (
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	collpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func newOut(t *testing.T, yaml string) *Output {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - " + yaml))
	if err != nil {
		t.Fatal(err)
	}
	o, err := New(&cfg.Outputs[0], pipeline.Env{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Registry: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.Close(context.Background()) })
	return o.(*Output)
}

func batch() []*telemetry.Metric {
	ts := time.Unix(1700000000, 0)
	a := telemetry.New("openconfig:/interfaces/interface", ts)
	a.Tags = map[string]string{"source": "r1", "interface_name": "Eth1"}
	a.Fields = map[string]any{"state/counters/in-octets": uint64(10), "state/oper-status": "UP", "state/mtu": int64(9000)}
	b := telemetry.New("openconfig:/interfaces/interface", ts)
	b.Tags = map[string]string{"source": "r1", "interface_name": "Eth2"}
	b.Fields = map[string]any{"state/counters/in-octets": uint64(1 << 63)}
	c := telemetry.New("/system/cpu", ts)
	c.Tags = map[string]string{"source": "r2"}
	c.Fields = map[string]any{"load": 0.5}
	return []*telemetry.Metric{a, b, c}
}

func check(t *testing.T, req *collpb.ExportMetricsServiceRequest) {
	t.Helper()
	if len(req.ResourceMetrics) != 2 {
		t.Fatalf("want 2 resources (one per device), got %d", len(req.ResourceMetrics))
	}
	r1 := req.ResourceMetrics[0]
	var inst string
	for _, a := range r1.Resource.Attributes {
		if a.Key == "service.instance.id" {
			inst = a.Value.GetStringValue()
		}
	}
	if inst != "r1" {
		t.Errorf("resource instance = %q", inst)
	}
	ms := r1.ScopeMetrics[0].Metrics
	if len(ms) != 2 { // in-octets, mtu (string dropped)
		t.Fatalf("metrics: %v", ms)
	}
	in := ms[0]
	if in.Name != "interfaces_interface_state_counters_in_octets" || len(in.GetGauge().DataPoints) != 2 {
		t.Fatalf("in-octets metric: %v", in)
	}
	dp := in.GetGauge().DataPoints[0]
	if dp.GetAsInt() != 10 || dp.TimeUnixNano != uint64(time.Unix(1700000000, 0).UnixNano()) ||
		len(dp.Attributes) != 1 || dp.Attributes[0].Key != "interface_name" {
		t.Errorf("datapoint: %v", dp)
	}
	if v := in.GetGauge().DataPoints[1].GetAsDouble(); v != float64(uint64(1<<63)) {
		t.Errorf("uint64 overflow not converted to double: %v", in.GetGauge().DataPoints[1])
	}
}

func TestHTTP(t *testing.T) {
	var got collpb.ExportMetricsServiceRequest
	var auth string
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		b, _ := io.ReadAll(zr)
		if err := proto.Unmarshal(b, &got); err != nil {
			t.Error(err)
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	o := newOut(t, `{type: otlp, protocol: http, endpoint: "`+srv.URL+`/api/v1/otlp/v1/metrics", headers: {Authorization: "Bearer t"}}`)
	if err := o.Write(context.Background(), batch()); err != nil {
		t.Fatal(err)
	}
	check(t, &got)
	if auth != "Bearer t" {
		t.Errorf("auth header %q", auth)
	}
	status = http.StatusBadRequest
	if err := o.Write(context.Background(), batch()); !pipeline.IsPermanent(err) {
		t.Errorf("400 should be permanent, got %v", err)
	}
	status = http.StatusServiceUnavailable
	if err := o.Write(context.Background(), batch()); err == nil || pipeline.IsPermanent(err) {
		t.Errorf("503 should be retryable, got %v", err)
	}
}

type collector struct {
	collpb.UnimplementedMetricsServiceServer
	mu  sync.Mutex
	got *collpb.ExportMetricsServiceRequest
	md  metadata.MD
}

func (c *collector) Export(ctx context.Context, r *collpb.ExportMetricsServiceRequest) (*collpb.ExportMetricsServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = r
	c.md, _ = metadata.FromIncomingContext(ctx)
	return &collpb.ExportMetricsServiceResponse{}, nil
}

func TestGRPC(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	srv := grpc.NewServer()
	c := &collector{}
	collpb.RegisterMetricsServiceServer(srv, c)
	go srv.Serve(lis)
	defer srv.Stop()

	o := newOut(t, `{type: otlp, endpoint: "`+lis.Addr().String()+`", headers: {x-tenant: acme}}`)
	if err := o.Write(context.Background(), batch()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	check(t, c.got)
	if v := c.md.Get("x-tenant"); len(v) != 1 || v[0] != "acme" {
		t.Errorf("metadata %v", c.md)
	}
}
