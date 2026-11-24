package prometheus

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func newOut(t *testing.T, extra string) *Output {
	t.Helper()
	cfg, err := config.Parse([]byte(`inputs: [{type: x}]
outputs:
  - {type: prometheus, listen: "127.0.0.1:0"` + extra + `}`))
	if err != nil {
		t.Fatal(err)
	}
	o, err := New(&cfg.Outputs[0], pipeline.Env{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Registry: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	out := o.(*Output)
	if err := out.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { out.Close(context.Background()) })
	return out
}

func scrape(t *testing.T, o *Output) string {
	t.Helper()
	resp, err := http.Get("http://" + o.Addr().String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	return string(b)
}

func TestExporter(t *testing.T) {
	o := newOut(t, `, string_fields: info`)
	m1 := telemetry.New("openconfig:/interfaces/interface", time.Now())
	m1.Tags = map[string]string{"source": "r1", "interface_name": "Eth1"}
	m1.Fields = map[string]any{"state/counters/in-octets": uint64(42), "state/oper-status": "UP", "state/enabled": true}
	// Same family, different label set (missing interface_name) must not break the scrape.
	m2 := telemetry.New("openconfig:/interfaces/interface", time.Now())
	m2.Tags = map[string]string{"source": "r2", "interface-description": "uplink"}
	m2.Fields = map[string]any{"state/counters/in-octets": int64(7)}
	if err := o.Write(context.Background(), []*telemetry.Metric{m1, m2}); err != nil {
		t.Fatal(err)
	}
	body := scrape(t, o)
	for _, want := range []string{
		`interfaces_interface_state_counters_in_octets{interface_description="",interface_name="Eth1",source="r1"} 42`,
		`interfaces_interface_state_counters_in_octets{interface_description="uplink",interface_name="",source="r2"} 7`,
		`interfaces_interface_state_enabled{interface_name="Eth1",source="r1"} 1`,
		`interfaces_interface_state_oper_status_info{interface_name="Eth1",source="r1",value="UP"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}

	// A string change replaces the info series rather than adding one.
	m1b := m1.Clone()
	m1b.Fields = map[string]any{"state/oper-status": "DOWN"}
	o.Write(context.Background(), []*telemetry.Metric{m1b})
	body = scrape(t, o)
	if strings.Contains(body, `value="UP"`) || !strings.Contains(body, `value="DOWN"`) {
		t.Errorf("info series not replaced:\n%s", body)
	}
}

func TestExpiry(t *testing.T) {
	o := newOut(t, `, expiry: 50ms`)
	m := telemetry.New("/x", time.Now())
	m.Fields["v"] = 1.0
	o.Write(context.Background(), []*telemetry.Metric{m})
	if !strings.Contains(scrape(t, o), "x_v 1") {
		t.Fatal("series missing")
	}
	time.Sleep(100 * time.Millisecond)
	if strings.Contains(scrape(t, o), "x_v") {
		t.Fatal("stale series not expired")
	}
}
