package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/input/ciscomdt"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestEndToEnd runs the real binary wiring: an IOS XR device dials out over
// TCP, metrics pass through processors, and land in Prometheus and a file.
func TestEndToEnd(t *testing.T) {
	dir := t.TempDir()
	mdt, prom, adm := freePort(t), freePort(t), freePort(t)
	dump := filepath.Join(dir, "dump.jsonl")
	cfgFile := filepath.Join(dir, "pipeline.yaml")
	t.Setenv("SITE", "dfw1")
	os.WriteFile(cfgFile, []byte(fmt.Sprintf(`
log: {level: warn, format: text}
admin: {listen: "%s"}
inputs:
  - {name: xr, type: cisco_mdt, transport: tcp, listen: "%s"}
processors:
  - {type: filter, include_names: ["generic-counters"]}
  - {type: tags, add: {site: "${SITE}"}, rename: {interface-name: interface}}
outputs:
  - {name: prom, type: prometheus, listen: "%s", flush_interval: 10ms}
  - {name: dump, type: file, path: "%s", flush_interval: 10ms}
`, adm, mdt, prom, dump)), 0o644)

	if code := run(context.Background(), []string{"-config", cfgFile, "-check"}, nil); code != 0 {
		t.Fatalf("-check failed with %d", code)
	}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan string, 1)
	done := make(chan int, 1)
	go func() { done <- run(ctx, []string{"-config", cfgFile}, started) }()
	<-started
	eventually(t, "readiness", func() bool { c, _ := get(t, "http://"+adm+"/readyz"); return c == 200 })

	counters, _ := os.ReadFile("../../internal/input/ciscomdt/testdata/generic_counters.kvgpb")
	memory, _ := os.ReadFile("../../internal/input/ciscomdt/testdata/memory_summary.kvgpb")
	var conn net.Conn
	eventually(t, "mdt listener", func() bool { var err error; conn, err = net.Dial("tcp", mdt); return err == nil })
	conn.Write(ciscomdt.EncodeST(counters, false))
	conn.Write(ciscomdt.EncodeST(memory, false)) // filtered out
	conn.Close()

	var body string
	eventually(t, "prometheus series", func() bool {
		_, body = get(t, "http://"+prom+"/metrics")
		return strings.Contains(body, "generic_counters_bytes_received{")
	})
	if !strings.Contains(body, `site="dfw1"`) || !strings.Contains(body, `interface="`) || !strings.Contains(body, `source="dr02"`) {
		t.Errorf("tags not applied:\n%.1500s", body)
	}
	if strings.Contains(body, "memory_summary") {
		t.Error("filter processor did not drop memory-summary")
	}
	_, self := get(t, "http://"+adm+"/metrics")
	if !strings.Contains(self, `pipeline_output_written_total{output="prom"} 8`) {
		t.Errorf("self metrics missing written count:\n%s", grep(self, "pipeline_"))
	}

	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("exit code %d", code)
	}
	f, _ := os.Open(dump)
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	lines := 0
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		lines++
	}
	if lines != 8 {
		t.Errorf("dump has %d lines, want 8", lines)
	}
}

func TestBadConfig(t *testing.T) {
	cfg := filepath.Join(t.TempDir(), "bad.yaml")
	os.WriteFile(cfg, []byte("inputs: [{type: gnmi}]\noutputs: [{type: file}]"), 0o644)
	if code := run(context.Background(), []string{"-config", cfg, "-check"}, nil); code == 0 {
		t.Fatal("expected failure for gnmi input without targets")
	}
}

func grep(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
