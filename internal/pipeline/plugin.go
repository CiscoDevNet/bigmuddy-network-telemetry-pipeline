// Package pipeline wires inputs, processors and outputs together.
//
//	inputs ──► router queue ──► processors ──┬─► output queue ─► batcher/retry ─► output A
//	(block)        (bounded,     (in order)   ├─► output queue ─► batcher/retry ─► output B
//	               backpressure)              └─► ...
//
// Inputs block when the router queue is full, which propagates backpressure
// to devices through gRPC/TCP flow control. Each output has its own bounded
// queue; when an output cannot keep up its queue fills and new metrics are
// dropped *for that output only* (and counted), so a slow Kafka cluster can
// never stall Prometheus or vice versa.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// Sink receives metrics from an input.
type Sink interface {
	// Emit hands a batch to the pipeline. It blocks while the router queue is
	// full and returns ctx.Err() if ctx is cancelled first. The caller must
	// not modify the metrics afterwards.
	Emit(ctx context.Context, batch []*telemetry.Metric) error
}

// Input produces metrics. Run blocks until ctx is cancelled. It should
// handle transient failures (device unreachable, stream reset) itself and
// only return an error for unrecoverable problems such as a listen failure.
type Input interface {
	Run(ctx context.Context, sink Sink) error
}

// Processor transforms a batch. It may modify metrics in place, drop them
// (by omitting them from the result) or add new ones.
type Processor interface {
	Process(batch []*telemetry.Metric) []*telemetry.Metric
}

// Output delivers metrics to a destination. Write is called from a single
// goroutine per output. Metrics are shared between outputs and must not be
// modified. Return Permanent(err) for errors that retrying cannot fix.
type Output interface {
	Write(ctx context.Context, batch []*telemetry.Metric) error
	Close(ctx context.Context) error
}

// Starter is optionally implemented by outputs that run a server (e.g. the
// Prometheus exporter) and need a lifecycle context.
type Starter interface {
	Start(ctx context.Context) error
}

// Env is passed to plugin factories.
type Env struct {
	Name     string
	Logger   *slog.Logger
	Registry prometheus.Registerer
}

type (
	InputFactory     func(p *config.Plugin, env Env) (Input, error)
	ProcessorFactory func(p *config.Plugin, env Env) (Processor, error)
	OutputFactory    func(p *config.Plugin, env Env) (Output, error)
)

var (
	regMu      sync.RWMutex
	inputs     = map[string]InputFactory{}
	processors = map[string]ProcessorFactory{}
	outputs    = map[string]OutputFactory{}
)

// RegisterInput makes an input type available to the configuration.
func RegisterInput(typ string, f InputFactory) { register(inputs, typ, f) }

// RegisterProcessor makes a processor type available.
func RegisterProcessor(typ string, f ProcessorFactory) { register(processors, typ, f) }

// RegisterOutput makes an output type available.
func RegisterOutput(typ string, f OutputFactory) { register(outputs, typ, f) }

func register[F any](m map[string]F, typ string, f F) {
	regMu.Lock()
	defer regMu.Unlock()
	if _, dup := m[typ]; dup {
		panic("pipeline: duplicate plugin type " + typ)
	}
	m[typ] = f
}

func lookup[F any](m map[string]F, kind, typ string) (F, error) {
	regMu.RLock()
	defer regMu.RUnlock()
	f, ok := m[typ]
	if !ok {
		var zero F
		known := make([]string, 0, len(m))
		for k := range m {
			known = append(known, k)
		}
		sort.Strings(known)
		return zero, fmt.Errorf("unknown %s type %q (available: %v)", kind, typ, known)
	}
	return f, nil
}

type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent marks err as not retryable (e.g. HTTP 400 from a TSDB).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
