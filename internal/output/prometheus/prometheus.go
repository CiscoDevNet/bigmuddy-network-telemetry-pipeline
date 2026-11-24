// Package prometheus exposes received telemetry on a scrape endpoint.
//
// The 2016 pipeline pushed to a Pushgateway to keep device timestamps;
// that pattern is now discouraged (it never forgets series and breaks
// staleness handling). Modern options are:
//
//   - this exporter (pull, with series expiry), or
//   - the otlp output pointed at Prometheus' native OTLP receiver
//     (/api/v1/otlp/v1/metrics) when push semantics are needed.
package prometheus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

func init() { pipeline.RegisterOutput("prometheus", New) }

// Config is the prometheus output's YAML.
type Config struct {
	Listen           string         `yaml:"listen"`
	Path             string         `yaml:"path"`
	Namespace        string         `yaml:"namespace"`
	Expiry           time.Duration  `yaml:"expiry"`            // drop series not updated for this long
	ExportTimestamps bool           `yaml:"export_timestamps"` // use device timestamps (off by default: breaks staleness)
	StringFields     string         `yaml:"string_fields"`     // drop (default) or info
	TLS              tlsutil.Config `yaml:"tls"`
}

type sample struct {
	name   string
	labels map[string]string
	value  float64
	ts     time.Time
	seen   time.Time
}

// Output is the Prometheus exporter.
type Output struct {
	cfg    Config
	log    *slog.Logger
	mu     sync.Mutex
	series map[string]*sample
	srv    *http.Server
	lis    net.Listener
	count  prometheus.GaugeFunc
}

// New builds the exporter.
func New(p *config.Plugin, env pipeline.Env) (pipeline.Output, error) {
	c := Config{Listen: ":9273", Path: "/metrics", Expiry: 5 * time.Minute, StringFields: "drop"}
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	if c.StringFields != "drop" && c.StringFields != "info" {
		return nil, fmt.Errorf("string_fields must be drop or info, got %q", c.StringFields)
	}
	o := &Output{cfg: c, log: env.Logger, series: map[string]*sample{}}
	o.count = prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "prometheus_exporter_series",
		Help: "Series currently exposed by the exporter."}, func() float64 {
		o.mu.Lock()
		defer o.mu.Unlock()
		return float64(len(o.series))
	})
	env.Registry.MustRegister(o.count)

	reg := prometheus.NewRegistry()
	reg.MustRegister(o)
	mux := http.NewServeMux()
	mux.Handle(c.Path, promhttp.HandlerFor(reg, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError,
		EnableOpenMetrics: true}))
	tc, err := c.TLS.Server()
	if err != nil {
		return nil, err
	}
	o.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second, TLSConfig: tc}
	return o, nil
}

// Start binds the listener and serves scrapes.
func (o *Output) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", o.cfg.Listen)
	if err != nil {
		return err
	}
	o.lis = lis
	o.log.Info("serving Prometheus metrics", "addr", lis.Addr().String(), "path", o.cfg.Path)
	go func() {
		var err error
		if o.srv.TLSConfig != nil {
			err = o.srv.ServeTLS(lis, "", "")
		} else {
			err = o.srv.Serve(lis)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			o.log.Error("prometheus exporter stopped", "err", err)
		}
	}()
	return nil
}

// Addr returns the bound address once started.
func (o *Output) Addr() net.Addr { return o.lis.Addr() }

// Write updates the series store.
func (o *Output) Write(_ context.Context, batch []*telemetry.Metric) error {
	now := time.Now()
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, m := range batch {
		labels := make(map[string]string, len(m.Tags))
		for k, v := range m.Tags {
			labels[telemetry.SanitizeLabel(k)] = v
		}
		base := m.Name
		if o.cfg.Namespace != "" {
			base = o.cfg.Namespace + "/" + base
		}
		for field, v := range m.Fields {
			name := telemetry.SanitizeName(base + "/" + field)
			val, ok := telemetry.ToFloat(v)
			lbl := labels
			if !ok {
				s, isStr := v.(string)
				if !isStr || o.cfg.StringFields != "info" {
					continue
				}
				name += "_info"
				lbl = make(map[string]string, len(labels)+1)
				for k, v := range labels {
					lbl[k] = v
				}
				lbl["value"] = s
				val = 1
			}
			key := seriesKey(name, labels)
			s := o.series[key]
			if s == nil {
				s = &sample{name: name}
				o.series[key] = s
			}
			s.labels, s.value, s.ts, s.seen = lbl, val, m.Timestamp, now
		}
	}
	return nil
}

func seriesKey(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteByte(0)
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	return b.String()
}

// Describe is intentionally empty: this is an unchecked collector whose
// series set changes with the data.
func (o *Output) Describe(chan<- *prometheus.Desc) {}

// Collect evicts stale series and emits the rest. All series of one metric
// family get the same label names (missing ones as ""), which Prometheus
// treats as absent, so devices that omit a key never break the scrape.
func (o *Output) Collect(ch chan<- prometheus.Metric) {
	o.mu.Lock()
	now := time.Now()
	families := map[string][]*sample{}
	for k, s := range o.series {
		if o.cfg.Expiry > 0 && now.Sub(s.seen) > o.cfg.Expiry {
			delete(o.series, k)
			continue
		}
		families[s.name] = append(families[s.name], s)
	}
	o.mu.Unlock()

	for name, ss := range families {
		set := map[string]struct{}{}
		for _, s := range ss {
			for k := range s.labels {
				set[k] = struct{}{}
			}
		}
		names := make([]string, 0, len(set))
		for k := range set {
			names = append(names, k)
		}
		sort.Strings(names)
		desc := prometheus.NewDesc(name, "Telemetry from "+name, names, nil)
		for _, s := range ss {
			vals := make([]string, len(names))
			for i, n := range names {
				vals[i] = s.labels[n]
			}
			m, err := prometheus.NewConstMetric(desc, prometheus.UntypedValue, s.value, vals...)
			if err != nil {
				continue
			}
			if o.cfg.ExportTimestamps {
				m = prometheus.NewMetricWithTimestamp(s.ts, m)
			}
			ch <- m
		}
	}
}

// Close stops the HTTP server.
func (o *Output) Close(ctx context.Context) error {
	return o.srv.Shutdown(ctx)
}
