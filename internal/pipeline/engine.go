package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// DrainTimeout bounds how long shutdown waits for output queues to flush.
var DrainTimeout = 30 * time.Second

type namedInput struct {
	name string
	in   Input
}

type namedProcessor struct {
	name string
	p    Processor
}

// Engine is a running pipeline.
type Engine struct {
	log        *slog.Logger
	inputs     []namedInput
	processors []namedProcessor
	outputs    []*outputWorker
	routerQ    chan routed
	m          *selfMetrics
	ready      atomic.Bool
}

type routed struct {
	input string
	batch []*telemetry.Metric
}

// New builds every plugin described by cfg. Nothing is started yet.
func New(cfg *config.Config, log *slog.Logger, reg prometheus.Registerer) (*Engine, error) {
	e := &Engine{log: log, routerQ: make(chan routed, cfg.Router.QueueSize)}
	e.m = newSelfMetrics(reg, func() float64 { return float64(len(e.routerQ)) })

	env := func(kind string, p *config.Plugin) Env {
		return Env{Name: p.Name, Logger: log.With(kind, p.Name, "type", p.Type),
			Registry: prometheus.WrapRegistererWith(prometheus.Labels{kind: p.Name}, reg)}
	}
	for i := range cfg.Inputs {
		p := &cfg.Inputs[i]
		f, err := lookup(inputs, "input", p.Type)
		if err != nil {
			return nil, err
		}
		in, err := f(p, env("input", p))
		if err != nil {
			return nil, fmt.Errorf("input %q: %w", p.Name, err)
		}
		e.inputs = append(e.inputs, namedInput{p.Name, in})
	}
	for i := range cfg.Processors {
		p := &cfg.Processors[i]
		f, err := lookup(processors, "processor", p.Type)
		if err != nil {
			return nil, err
		}
		pr, err := f(p, env("processor", p))
		if err != nil {
			return nil, fmt.Errorf("processor %q: %w", p.Name, err)
		}
		e.processors = append(e.processors, namedProcessor{p.Name, pr})
	}
	for i := range cfg.Outputs {
		p := &cfg.Outputs[i]
		f, err := lookup(outputs, "output", p.Type)
		if err != nil {
			return nil, err
		}
		o, err := f(p, env("output", p))
		if err != nil {
			return nil, fmt.Errorf("output %q: %w", p.Name, err)
		}
		e.outputs = append(e.outputs, &outputWorker{name: p.Name, out: o, d: p.Delivery,
			q: make(chan *telemetry.Metric, p.Delivery.QueueSize), m: e.m, log: env("output", p).Logger})
	}
	return e, nil
}

// Ready reports whether all stages have started.
func (e *Engine) Ready() bool { return e.ready.Load() }

// Run starts the pipeline and blocks until ctx is cancelled (graceful
// shutdown: inputs stop, queues drain, outputs flush) or an input fails
// fatally.
func (e *Engine) Run(ctx context.Context) error {
	// Outputs run on their own context so they keep draining after inputs
	// have been told to stop.
	outCtx, cancelOut := context.WithCancel(context.Background())
	defer cancelOut()
	for _, w := range e.outputs {
		if s, ok := w.out.(Starter); ok {
			if err := s.Start(outCtx); err != nil {
				return fmt.Errorf("output %q: start: %w", w.name, err)
			}
		}
	}
	var outWG sync.WaitGroup
	for _, w := range e.outputs {
		outWG.Add(1)
		go func() { defer outWG.Done(); w.run(outCtx) }()
	}

	routerDone := make(chan struct{})
	go func() { defer close(routerDone); e.route() }()

	inCtx, cancelIn := context.WithCancel(ctx)
	defer cancelIn()
	var inWG sync.WaitGroup
	fatal := make(chan error, len(e.inputs))
	for _, in := range e.inputs {
		inWG.Add(1)
		go func() {
			defer inWG.Done()
			err := in.in.Run(inCtx, &sink{e: e, name: in.name})
			if err != nil && !errors.Is(err, context.Canceled) {
				fatal <- fmt.Errorf("input %q: %w", in.name, err)
			}
		}()
	}
	e.ready.Store(true)
	e.log.Info("pipeline started", "inputs", len(e.inputs), "processors", len(e.processors), "outputs", len(e.outputs))

	var runErr error
	select {
	case <-ctx.Done():
		e.log.Info("shutting down")
	case runErr = <-fatal:
		e.log.Error("input failed, shutting down", "err", runErr)
	}
	e.ready.Store(false)

	cancelIn()
	inWG.Wait()
	close(e.routerQ)
	<-routerDone
	for _, w := range e.outputs {
		close(w.q)
	}
	drained := make(chan struct{})
	go func() { outWG.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(DrainTimeout):
		e.log.Warn("drain timeout exceeded; abandoning queued metrics")
		cancelOut()
		<-drained
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, w := range e.outputs {
		if err := w.out.Close(closeCtx); err != nil {
			e.log.Warn("output close failed", "output", w.name, "err", err)
		}
	}
	e.log.Info("pipeline stopped")
	return runErr
}

type sink struct {
	e    *Engine
	name string
}

func (s *sink) Emit(ctx context.Context, batch []*telemetry.Metric) error {
	if len(batch) == 0 {
		return nil
	}
	select {
	case s.e.routerQ <- routed{s.name, batch}:
		s.e.m.received.WithLabelValues(s.name).Add(float64(len(batch)))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) route() {
	for r := range e.routerQ {
		batch := r.batch
		n := len(batch)
		for _, p := range e.processors {
			batch = p.p.Process(batch)
			if len(batch) == 0 {
				break
			}
		}
		e.m.filtered.Add(float64(n - len(batch)))
		e.m.processed.Add(float64(len(batch)))
		for _, w := range e.outputs {
			w.enqueue(batch)
		}
	}
}

type outputWorker struct {
	name string
	out  Output
	d    config.Delivery
	q    chan *telemetry.Metric
	m    *selfMetrics
	log  *slog.Logger
}

func (w *outputWorker) enqueue(batch []*telemetry.Metric) {
	var dropped int
	for _, m := range batch {
		select {
		case w.q <- m:
		default:
			dropped++
		}
	}
	if dropped > 0 {
		w.m.dropped.WithLabelValues(w.name, "queue_full").Add(float64(dropped))
	}
	w.m.enqueued.WithLabelValues(w.name).Add(float64(len(batch) - dropped))
}

func (w *outputWorker) run(ctx context.Context) {
	t := time.NewTicker(w.d.FlushInterval)
	defer t.Stop()
	buf := make([]*telemetry.Metric, 0, w.d.BatchSize)
	flush := func() {
		if len(buf) > 0 {
			w.write(ctx, buf)
			buf = make([]*telemetry.Metric, 0, w.d.BatchSize)
		}
	}
	for {
		select {
		case m, ok := <-w.q:
			if !ok {
				flush()
				return
			}
			buf = append(buf, m)
			if len(buf) >= w.d.BatchSize {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}

func (w *outputWorker) write(ctx context.Context, batch []*telemetry.Metric) {
	backoff := 100 * time.Millisecond
	for attempt := 0; ; attempt++ {
		wctx, cancel := context.WithTimeout(ctx, w.d.WriteTimeout)
		start := time.Now()
		err := w.out.Write(wctx, batch)
		cancel()
		w.m.writeDuration.WithLabelValues(w.name).Observe(time.Since(start).Seconds())
		if err == nil {
			w.m.written.WithLabelValues(w.name).Add(float64(len(batch)))
			w.m.batchSize.WithLabelValues(w.name).Observe(float64(len(batch)))
			return
		}
		w.m.writeErrors.WithLabelValues(w.name).Inc()
		if IsPermanent(err) || attempt >= w.d.MaxRetries || ctx.Err() != nil {
			w.log.Warn("dropping batch after write failure", "metrics", len(batch),
				"attempts", attempt+1, "permanent", IsPermanent(err), "err", err)
			w.m.dropped.WithLabelValues(w.name, "write_failed").Add(float64(len(batch)))
			return
		}
		w.log.Debug("write failed, retrying", "attempt", attempt+1, "backoff", backoff, "err", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}
