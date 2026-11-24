package processor

import (
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func build(t *testing.T, yaml string) pipeline.Processor {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs: [{type: x}]\noutputs: [{type: y}]\nprocessors:\n  - " + yaml))
	if err != nil {
		t.Fatal(err)
	}
	p := &cfg.Processors[0]
	env := pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()}
	var f func(*config.Plugin, pipeline.Env) (pipeline.Processor, error)
	switch p.Type {
	case "filter":
		f = newFilter
	case "tags":
		f = newTags
	case "rename":
		f = newRename
	}
	pr, err := f(p, env)
	if err != nil {
		t.Fatal(err)
	}
	return pr
}

func metric(name, src string, fields map[string]any) *telemetry.Metric {
	m := telemetry.New(name, time.Now())
	m.Tags[telemetry.TagSource] = src
	for k, v := range fields {
		m.Fields[k] = v
	}
	return m
}

func TestFilter(t *testing.T) {
	p := build(t, `type: filter
    include_names: ["interfaces"]
    exclude_tags: {source: ["^lab-"]}
    drop_fields: ["description$"]
    drop_string_fields: true`)
	in := []*telemetry.Metric{
		metric("/interfaces/interface", "r1", map[string]any{"in": int64(1), "description": "x", "state": "UP"}),
		metric("/system", "r1", map[string]any{"a": int64(1)}),
		metric("/interfaces/interface", "lab-r2", map[string]any{"in": int64(1)}),
		metric("/interfaces/interface", "r3", map[string]any{"only": "strings"}),
	}
	out := p.Process(in)
	if len(out) != 1 || out[0].Source() != "r1" || len(out[0].Fields) != 1 || out[0].Fields["in"] != int64(1) {
		t.Fatalf("unexpected %+v", out)
	}
}

func TestTags(t *testing.T) {
	p := build(t, `type: tags
    add: {env: prod, source: ignored}
    sources: {r1: {site: dfw1, role: spine}}
    rename: {interface_name: ifname}
    drop: [subscription]`)
	m := metric("x", "r1", map[string]any{"v": int64(1)})
	m.Tags["interface_name"] = "Eth1"
	m.Tags["subscription"] = "s"
	p.Process([]*telemetry.Metric{m})
	want := map[string]string{"source": "r1", "env": "prod", "site": "dfw1", "role": "spine", "ifname": "Eth1"}
	if len(m.Tags) != len(want) {
		t.Fatalf("got %v", m.Tags)
	}
	for k, v := range want {
		if m.Tags[k] != v {
			t.Errorf("%s=%q want %q", k, m.Tags[k], v)
		}
	}
}

func TestRename(t *testing.T) {
	p := build(t, `type: rename
    names: [{match: "^Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters$", replace: "xr_if_counters"}]
    fields: [{match: "^state/counters/", replace: ""}]`)
	m := metric("Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters", "r", map[string]any{"state/counters/in-octets": int64(1)})
	p.Process([]*telemetry.Metric{m})
	if m.Name != "xr_if_counters" || m.Fields["in-octets"] != int64(1) {
		t.Errorf("got %+v", m)
	}
}

func TestBadRegex(t *testing.T) {
	cfg, _ := config.Parse([]byte("inputs: [{type: x}]\noutputs: [{type: y}]\nprocessors: [{type: filter, include_names: ['(']}]"))
	if _, err := newFilter(&cfg.Processors[0], pipeline.Env{}); err == nil {
		t.Fatal("expected regex error")
	}
}
