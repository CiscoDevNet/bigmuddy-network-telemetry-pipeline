package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// --- fakes -------------------------------------------------------------

type genInput struct{ n int }

func (g *genInput) Run(ctx context.Context, s Sink) error {
	for i := 0; i < g.n; i++ {
		m := telemetry.New("m", time.Now())
		m.Fields["i"] = int64(i)
		if err := s.Emit(ctx, []*telemetry.Metric{m}); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return nil
}

type dropOdd struct{}

func (dropOdd) Process(b []*telemetry.Metric) []*telemetry.Metric {
	out := b[:0]
	for _, m := range b {
		if m.Fields["i"].(int64)%2 == 0 {
			out = append(out, m)
		}
	}
	return out
}

type recOutput struct {
	mu       sync.Mutex
	got      []*telemetry.Metric
	failN    atomic.Int32 // fail this many writes first
	perm     bool
	block    chan struct{}
	closed   atomic.Bool
	attempts atomic.Int32
}

func (o *recOutput) Write(ctx context.Context, b []*telemetry.Metric) error {
	o.attempts.Add(1)
	if o.block != nil {
		select {
		case <-o.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if o.failN.Load() > 0 {
		o.failN.Add(-1)
		if o.perm {
			return Permanent(errors.New("bad request"))
		}
		return errors.New("transient")
	}
	o.mu.Lock()
	o.got = append(o.got, b...)
	o.mu.Unlock()
	return nil
}
func (o *recOutput) Close(context.Context) error { o.closed.Store(true); return nil }
func (o *recOutput) count() int                  { o.mu.Lock(); defer o.mu.Unlock(); return len(o.got) }

var testPlugins = struct {
	sync.Mutex
	outs map[string]*recOutput
	n    int
}{outs: map[string]*recOutput{}}

func init() {
	RegisterInput("test_gen", func(p *config.Plugin, _ Env) (Input, error) {
		return &genInput{n: testPlugins.n}, nil
	})
	RegisterProcessor("test_drop_odd", func(*config.Plugin, Env) (Processor, error) { return dropOdd{}, nil })
	RegisterOutput("test_rec", func(p *config.Plugin, _ Env) (Output, error) {
		testPlugins.Lock()
		defer testPlugins.Unlock()
		return testPlugins.outs[p.Name], nil
	})
}

func build(t *testing.T, yaml string, n int, outs map[string]*recOutput) (*Engine, *prometheus.Registry) {
	t.Helper()
	testPlugins.Lock()
	testPlugins.n = n
	testPlugins.outs = outs
	testPlugins.Unlock()
	cfg, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	e, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), reg)
	if err != nil {
		t.Fatal(err)
	}
	return e, reg
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for condition")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// --- tests -------------------------------------------------------------

func TestFanOutProcessorsAndDrain(t *testing.T) {
	a, b := &recOutput{}, &recOutput{}
	e, reg := build(t, `
inputs: [{type: test_gen}]
processors: [{type: test_drop_odd}]
outputs:
  - {name: a, type: test_rec, batch_size: 7, flush_interval: 1h}
  - {name: b, type: test_rec, batch_size: 1000, flush_interval: 1h}
`, 100, map[string]*recOutput{"a": a, "b": b})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- e.Run(ctx) }()
	waitFor(t, func() bool { return a.count() >= 49 }) // full batches of 7 arrive before shutdown
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// After shutdown, partial batches must have been flushed.
	if a.count() != 50 || b.count() != 50 {
		t.Fatalf("got a=%d b=%d, want 50 each", a.count(), b.count())
	}
	if !a.closed.Load() || !b.closed.Load() {
		t.Fatal("outputs not closed")
	}
	if v := testutil.ToFloat64(e.m.filtered); v != 50 {
		t.Errorf("filtered=%v", v)
	}
	if n, _ := testutil.GatherAndCount(reg, "pipeline_output_written_total"); n != 2 {
		t.Errorf("expected 2 written series, got %d", n)
	}
}

func TestSlowOutputIsIsolated(t *testing.T) {
	slow := &recOutput{block: make(chan struct{})}
	fast := &recOutput{}
	e, _ := build(t, `
inputs: [{type: test_gen}]
outputs:
  - {name: slow, type: test_rec, queue_size: 10, batch_size: 1, max_retries: -1}
  - {name: fast, type: test_rec, batch_size: 10, flush_interval: 10ms}
`, 500, map[string]*recOutput{"slow": slow, "fast": fast})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- e.Run(ctx) }()

	waitFor(t, func() bool { return fast.count() == 500 })
	dropped := testutil.ToFloat64(e.m.dropped.WithLabelValues("slow", "queue_full"))
	if dropped < 400 {
		t.Errorf("expected slow output to drop most metrics, dropped=%v", dropped)
	}
	close(slow.block)
	cancel()
	<-done
}

func TestRetryAndPermanent(t *testing.T) {
	transient := &recOutput{}
	transient.failN.Store(2)
	perm := &recOutput{perm: true}
	perm.failN.Store(1)
	e, _ := build(t, `
inputs: [{type: test_gen}]
outputs:
  - {name: t, type: test_rec, batch_size: 5, max_retries: 3}
  - {name: p, type: test_rec, batch_size: 5, max_retries: 3}
`, 5, map[string]*recOutput{"t": transient, "p": perm})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- e.Run(ctx) }()
	waitFor(t, func() bool { return transient.count() == 5 })
	waitFor(t, func() bool { return perm.attempts.Load() >= 1 })
	cancel()
	<-done
	if transient.attempts.Load() != 3 {
		t.Errorf("transient attempts=%d want 3", transient.attempts.Load())
	}
	if perm.attempts.Load() != 1 || perm.count() != 0 {
		t.Errorf("permanent error should not be retried: attempts=%d", perm.attempts.Load())
	}
	if v := testutil.ToFloat64(e.m.dropped.WithLabelValues("p", "write_failed")); v != 5 {
		t.Errorf("write_failed drops=%v", v)
	}
}

func TestUnknownPluginType(t *testing.T) {
	cfg, _ := config.Parse([]byte("inputs: [{type: nope}]\noutputs: [{type: test_rec}]"))
	if _, err := New(cfg, slog.Default(), prometheus.NewRegistry()); err == nil {
		t.Fatal("expected unknown type error")
	}
}
