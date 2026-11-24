package influxdb

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func metric() *telemetry.Metric {
	m := telemetry.New("openconfig:/interfaces/interface", time.Unix(0, 1700000000000000000))
	m.Tags = map[string]string{"source": "r 1", "interface_name": "Eth1,x=y", "empty": ""}
	m.Fields = map[string]any{"in-octets": uint64(1<<63 + 5), "mtu": int64(9000), "load": 0.25,
		"up": true, "descr": `say "hi" \o/`, "nan": math.NaN()}
	return m
}

func TestLineProtocol(t *testing.T) {
	var b bytes.Buffer
	AppendLine(&b, metric(), false)
	want := `openconfig:/interfaces/interface,interface_name=Eth1\,x\=y,source=r\ 1 descr="say \"hi\" \\o/",in-octets=9223372036854775813u,load=0.25,mtu=9000i,up=true 1700000000000000000` + "\n"
	if b.String() != want {
		t.Fatalf("got  %s\nwant %s", b.String(), want)
	}
	b.Reset()
	AppendLine(&b, metric(), true)
	if !bytes.Contains(b.Bytes(), []byte("in-octets=9.223372036854776e+18,")) {
		t.Errorf("v1 should not use u suffix: %s", b.String())
	}
	b.Reset()
	empty := telemetry.New("x", time.Now())
	empty.Fields["s"] = []int{1}
	AppendLine(&b, empty, false)
	if b.Len() != 0 {
		t.Errorf("point without fields should be skipped: %q", b.String())
	}
}

func newOut(t *testing.T, yaml string) pipeline.Output {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - " + yaml))
	if err != nil {
		t.Fatal(err)
	}
	o, err := New(&cfg.Outputs[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestWriteV2AndV1(t *testing.T) {
	var path, query, auth, body string
	code := 204
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, query, auth = r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization")
		var rd io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			rd, _ = gzip.NewReader(r.Body)
		}
		b, _ := io.ReadAll(rd)
		body = string(b)
		w.WriteHeader(code)
	}))
	defer srv.Close()

	t.Setenv("INFLUX_TOKEN", "tok")
	o := newOut(t, `{type: influxdb, url: "`+srv.URL+`/", org: acme, bucket: telemetry, token: "${INFLUX_TOKEN}"}`)
	if err := o.Write(context.Background(), []*telemetry.Metric{metric()}); err != nil {
		t.Fatal(err)
	}
	if path != "/api/v2/write" || query != "bucket=telemetry&org=acme&precision=ns" || auth != "Token tok" || len(body) == 0 {
		t.Errorf("v2 request: %s ? %s auth=%q body=%q", path, query, auth, body)
	}
	code = 400
	if err := o.Write(context.Background(), []*telemetry.Metric{metric()}); !pipeline.IsPermanent(err) {
		t.Errorf("400 must be permanent: %v", err)
	}
	code = 503
	if err := o.Write(context.Background(), []*telemetry.Metric{metric()}); err == nil || pipeline.IsPermanent(err) {
		t.Errorf("503 must be retryable: %v", err)
	}

	code = 204
	o = newOut(t, `{type: influxdb, url: "`+srv.URL+`", database: telem, retention_policy: autogen, username: u, password: p, gzip: false}`)
	if err := o.Write(context.Background(), []*telemetry.Metric{metric()}); err != nil {
		t.Fatal(err)
	}
	if path != "/write" || query != "db=telem&precision=ns&rp=autogen" || auth == "" {
		t.Errorf("v1 request: %s ? %s auth=%q", path, query, auth)
	}
}

func TestConfigErrors(t *testing.T) {
	for _, y := range []string{
		`{type: influxdb, bucket: b}`,
		`{type: influxdb, url: "http://x"}`,
		`{type: influxdb, url: "http://x", bucket: b, database: d}`,
	} {
		cfg, _ := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - " + y))
		if _, err := New(&cfg.Outputs[0], pipeline.Env{}); err == nil {
			t.Errorf("expected error for %s", y)
		}
	}
}
