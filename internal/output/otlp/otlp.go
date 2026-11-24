// Package otlp exports metrics using the OpenTelemetry Protocol, the
// industry-standard telemetry wire format. Targets include the
// OpenTelemetry Collector, Prometheus 3.x (native OTLP receiver), Grafana
// Mimir/Cloud, Datadog, Elastic, Splunk, New Relic, Honeycomb, etc.
//
// Each device becomes an OTel Resource (service.instance.id / host.name =
// source), each numeric field a Gauge data point with the metric's tags as
// attributes.
package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	collpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

func init() { pipeline.RegisterOutput("otlp", New) }

// Version is reported as the instrumentation scope version.
var Version = "dev"

// Config is the otlp output's YAML.
type Config struct {
	Protocol           string            `yaml:"protocol"` // grpc (default) or http
	Endpoint           string            `yaml:"endpoint"` // grpc: host:port; http: full URL
	Headers            map[string]string `yaml:"headers"`
	Compression        string            `yaml:"compression"` // gzip (default) or none
	TLS                tlsutil.Config    `yaml:"tls"`
	ResourceAttributes map[string]string `yaml:"resource_attributes"`
}

// Output is the OTLP exporter.
type Output struct {
	cfg    Config
	log    *slog.Logger
	conn   *grpc.ClientConn
	client collpb.MetricsServiceClient
	http   *http.Client
}

// New builds the exporter.
func New(p *config.Plugin, env pipeline.Env) (pipeline.Output, error) {
	c := Config{Protocol: "grpc", Compression: "gzip"}
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	if c.Endpoint == "" {
		return nil, errors.New("endpoint is required")
	}
	if c.Compression != "gzip" && c.Compression != "none" {
		return nil, fmt.Errorf("compression must be gzip or none")
	}
	tc, err := c.TLS.Client()
	if err != nil {
		return nil, err
	}
	o := &Output{cfg: c, log: env.Logger}
	switch c.Protocol {
	case "grpc":
		creds := insecure.NewCredentials()
		if tc != nil {
			creds = credentials.NewTLS(tc)
		}
		opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
		if c.Compression == "gzip" {
			opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(grpcgzip.Name)))
		}
		if o.conn, err = grpc.NewClient(c.Endpoint, opts...); err != nil {
			return nil, err
		}
		o.client = collpb.NewMetricsServiceClient(o.conn)
	case "http":
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = tc
		o.http = &http.Client{Transport: tr}
	default:
		return nil, fmt.Errorf("protocol must be grpc or http, got %q", c.Protocol)
	}
	return o, nil
}

// Write exports a batch.
func (o *Output) Write(ctx context.Context, batch []*telemetry.Metric) error {
	req := o.build(batch)
	if len(req.ResourceMetrics) == 0 {
		return nil
	}
	if o.client != nil {
		return o.writeGRPC(ctx, req)
	}
	return o.writeHTTP(ctx, req)
}

func (o *Output) writeGRPC(ctx context.Context, req *collpb.ExportMetricsServiceRequest) error {
	if len(o.cfg.Headers) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, metadata.New(o.cfg.Headers))
	}
	resp, err := o.client.Export(ctx, req)
	if err != nil {
		switch status.Code(err) {
		case codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied, codes.Unimplemented:
			return pipeline.Permanent(err)
		}
		return err
	}
	if ps := resp.GetPartialSuccess(); ps.GetRejectedDataPoints() > 0 {
		o.log.Warn("OTLP receiver rejected data points", "rejected", ps.GetRejectedDataPoints(), "msg", ps.GetErrorMessage())
	}
	return nil
}

func (o *Output) writeHTTP(ctx context.Context, req *collpb.ExportMetricsServiceRequest) error {
	body, err := proto.Marshal(req)
	if err != nil {
		return pipeline.Permanent(err)
	}
	var rd io.Reader = bytes.NewReader(body)
	if o.cfg.Compression == "gzip" {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		zw.Write(body)
		zw.Close()
		rd = &buf
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, o.cfg.Endpoint, rd)
	if err != nil {
		return pipeline.Permanent(err)
	}
	hr.Header.Set("Content-Type", "application/x-protobuf")
	if o.cfg.Compression == "gzip" {
		hr.Header.Set("Content-Encoding", "gzip")
	}
	for k, v := range o.cfg.Headers {
		hr.Header.Set(k, v)
	}
	resp, err := o.http.Do(hr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("OTLP HTTP %d: %s", resp.StatusCode, msg)
	default:
		return pipeline.Permanent(fmt.Errorf("OTLP HTTP %d: %s", resp.StatusCode, msg))
	}
}

func (o *Output) build(batch []*telemetry.Metric) *collpb.ExportMetricsServiceRequest {
	type resState struct {
		rm      *resourcepb.Resource
		metrics map[string]*metricspb.Metric
		order   []string
	}
	resources := map[string]*resState{}
	var resOrder []string

	for _, m := range batch {
		src := m.Source()
		rs := resources[src]
		if rs == nil {
			attrs := []*commonpb.KeyValue{
				kv("service.name", "network-telemetry"),
				kv("service.instance.id", src),
				kv("host.name", src),
			}
			for k, v := range o.cfg.ResourceAttributes {
				attrs = append(attrs, kv(k, v))
			}
			rs = &resState{rm: &resourcepb.Resource{Attributes: attrs}, metrics: map[string]*metricspb.Metric{}}
			resources[src] = rs
			resOrder = append(resOrder, src)
		}
		attrs := make([]*commonpb.KeyValue, 0, len(m.Tags))
		for _, k := range m.SortedTagKeys() {
			if k != telemetry.TagSource {
				attrs = append(attrs, kv(k, m.Tags[k]))
			}
		}
		ts := uint64(m.Timestamp.UnixNano())
		for _, f := range m.SortedFieldKeys() {
			dp := &metricspb.NumberDataPoint{TimeUnixNano: ts, Attributes: attrs}
			switch v := m.Fields[f].(type) {
			case int64:
				dp.Value = &metricspb.NumberDataPoint_AsInt{AsInt: v}
			case uint64:
				if v <= 1<<63-1 {
					dp.Value = &metricspb.NumberDataPoint_AsInt{AsInt: int64(v)}
				} else {
					dp.Value = &metricspb.NumberDataPoint_AsDouble{AsDouble: float64(v)}
				}
			default:
				fv, ok := telemetry.ToFloat(v)
				if !ok {
					continue // strings are not metrics
				}
				dp.Value = &metricspb.NumberDataPoint_AsDouble{AsDouble: fv}
			}
			name := telemetry.SanitizeName(m.Name + "/" + f)
			om := rs.metrics[name]
			if om == nil {
				om = &metricspb.Metric{Name: name, Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{}}}
				rs.metrics[name] = om
				rs.order = append(rs.order, name)
			}
			g := om.GetGauge()
			g.DataPoints = append(g.DataPoints, dp)
		}
	}

	req := &collpb.ExportMetricsServiceRequest{}
	for _, src := range resOrder {
		rs := resources[src]
		if len(rs.order) == 0 {
			continue
		}
		sm := &metricspb.ScopeMetrics{Scope: &commonpb.InstrumentationScope{
			Name: "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline", Version: Version}}
		for _, n := range rs.order {
			sm.Metrics = append(sm.Metrics, rs.metrics[n])
		}
		req.ResourceMetrics = append(req.ResourceMetrics, &metricspb.ResourceMetrics{
			Resource: rs.rm, ScopeMetrics: []*metricspb.ScopeMetrics{sm}})
	}
	return req
}

func kv(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

// Close releases the gRPC connection.
func (o *Output) Close(context.Context) error {
	if o.conn != nil {
		return o.conn.Close()
	}
	return nil
}
