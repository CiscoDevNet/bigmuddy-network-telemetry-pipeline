package gnmi

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gpb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// fakeTarget is a minimal gNMI server that checks credentials, sends one
// notification + sync, and then either holds the stream or fails it.
type fakeTarget struct {
	gpb.UnimplementedGNMIServer
	sessions  atomic.Int32
	failFirst bool
	lastReq   atomic.Pointer[gpb.SubscribeRequest]
}

func (f *fakeTarget) Subscribe(s grpc.BidiStreamingServer[gpb.SubscribeRequest, gpb.SubscribeResponse]) error {
	md, _ := metadata.FromIncomingContext(s.Context())
	if u := md.Get("username"); len(u) == 0 || u[0] != "admin" || md.Get("password")[0] != "pw" {
		return status.Error(codes.Unauthenticated, "bad credentials")
	}
	req, err := s.Recv()
	if err != nil {
		return err
	}
	f.lastReq.Store(req)
	n := f.sessions.Add(1)
	upd := &gpb.Notification{
		Timestamp: time.Now().UnixNano(),
		Prefix:    &gpb.Path{Elem: []*gpb.PathElem{{Name: "interfaces"}}},
		Update: []*gpb.Update{{
			Path: &gpb.Path{Elem: []*gpb.PathElem{{Name: "interface", Key: map[string]string{"name": "Eth1"}},
				{Name: "state"}, {Name: "counters"}, {Name: "in-octets"}}},
			Val: &gpb.TypedValue{Value: &gpb.TypedValue_UintVal{UintVal: uint64(n)}},
		}},
	}
	if err := s.Send(&gpb.SubscribeResponse{Response: &gpb.SubscribeResponse_Update{Update: upd}}); err != nil {
		return err
	}
	if err := s.Send(&gpb.SubscribeResponse{Response: &gpb.SubscribeResponse_SyncResponse{SyncResponse: true}}); err != nil {
		return err
	}
	if f.failFirst && n == 1 {
		return status.Error(codes.Unavailable, "simulated reboot")
	}
	<-s.Context().Done()
	return nil
}

type chanSink struct{ ch chan *telemetry.Metric }

func (c chanSink) Emit(ctx context.Context, b []*telemetry.Metric) error {
	for _, m := range b {
		select {
		case c.ch <- m:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func startFake(t *testing.T, f *fakeTarget) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	gpb.RegisterGNMIServer(srv, f)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func newInput(t *testing.T, yamlCfg string) (*Input, *prometheus.Registry) {
	t.Helper()
	cfg, err := config.Parse([]byte(yamlCfg))
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	in, err := New(&cfg.Inputs[0], pipeline.Env{Name: "g", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	return in.(*Input), reg
}

func TestSubscribeAndRedial(t *testing.T) {
	f := &fakeTarget{failFirst: true}
	addr := startFake(t, f)
	in, reg := newInput(t, `
inputs:
  - type: gnmi
    targets: [{address: "`+addr+`", name: r1}]
    username: admin
    password: pw
    redial_min: 10ms
    redial_max: 20ms
    subscriptions:
      - {name: counters, path: "/interfaces/interface/state/counters", mode: sample, sample_interval: 10s}
      - {name: status, path: "/interfaces/interface/state/oper-status", mode: on_change}
outputs: [{type: file}]
`)
	ctx, cancel := context.WithCancel(context.Background())
	sink := chanSink{make(chan *telemetry.Metric, 10)}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = in.Run(ctx, sink) }()

	var got []*telemetry.Metric
	for len(got) < 2 {
		select {
		case m := <-sink.ch:
			got = append(got, m)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for metrics")
		}
	}
	cancel()
	wg.Wait()

	if f.sessions.Load() < 2 {
		t.Fatalf("expected reconnect, sessions=%d", f.sessions.Load())
	}
	m := got[1]
	if m.Source() != "r1" || m.Tags[telemetry.TagSubscription] != "counters" ||
		m.Tags["interface_name"] != "Eth1" || m.Fields["state/counters/in-octets"] != uint64(2) {
		t.Errorf("unexpected metric %+v", m)
	}
	req := f.lastReq.Load().GetSubscribe()
	if len(req.Subscription) != 2 || req.Subscription[0].SampleInterval != uint64(10*time.Second) ||
		req.Subscription[1].Mode != gpb.SubscriptionMode_ON_CHANGE || req.Mode != gpb.SubscriptionList_STREAM {
		t.Errorf("unexpected subscribe request %v", req)
	}
	if v := testutil.ToFloat64(in.reconnects.WithLabelValues("r1")); v < 1 {
		t.Errorf("session failures metric = %v", v)
	}
	if n, _ := testutil.GatherAndCount(reg, "gnmi_sync_responses_total"); n != 1 {
		t.Errorf("sync metric missing")
	}
}

func TestBadCredentialsKeepsRetrying(t *testing.T) {
	f := &fakeTarget{}
	addr := startFake(t, f)
	in, _ := newInput(t, `
inputs:
  - type: gnmi
    targets: ["`+addr+`"]
    username: admin
    password: wrong
    redial_min: 5ms
    redial_max: 10ms
    subscriptions: [{path: /a, sample_interval: 1s}]
outputs: [{type: file}]
`)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := in.Run(ctx, chanSink{make(chan *telemetry.Metric)}); err != nil {
		t.Fatal(err)
	}
	if v := testutil.ToFloat64(in.reconnects.WithLabelValues("127.0.0.1")); v < 2 {
		t.Errorf("expected repeated failures, got %v", v)
	}
	if f.sessions.Load() != 0 {
		t.Error("unauthenticated session should not have been accepted")
	}
}

func TestConfigErrors(t *testing.T) {
	bad := []string{
		`targets: []`,
		`targets: [a:1]`,
		`targets: ["nohostport"]
subscriptions: [{path: /a, sample_interval: 1s}]`,
		`targets: [a:1]
subscriptions: [{path: /a}]`, // sample without interval
		`targets: [a:1]
subscriptions: [{path: /a, mode: poll}]`,
		`targets: [a:1]
encoding: xml
subscriptions: [{path: /a, mode: on_change}]`,
	}
	for _, b := range bad {
		cfg, err := config.Parse([]byte("inputs:\n  - type: gnmi\n" + indent(b) + "\noutputs: [{type: file}]"))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		_, err = New(&cfg.Inputs[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()})
		if err == nil {
			t.Errorf("expected error for %q", b)
		}
	}
}

func indent(s string) string {
	out := "    "
	for _, r := range s {
		out += string(r)
		if r == '\n' {
			out += "    "
		}
	}
	return out
}
