package pipeline

import "github.com/prometheus/client_golang/prometheus"

// selfMetrics are the pipeline's own health metrics, exposed on the admin
// endpoint. Plugins register additional metrics via Env.Registry.
type selfMetrics struct {
	received      *prometheus.CounterVec
	processed     prometheus.Counter
	filtered      prometheus.Counter
	routerQueue   prometheus.GaugeFunc
	enqueued      *prometheus.CounterVec
	written       *prometheus.CounterVec
	dropped       *prometheus.CounterVec
	writeErrors   *prometheus.CounterVec
	writeDuration *prometheus.HistogramVec
	batchSize     *prometheus.HistogramVec
}

func newSelfMetrics(reg prometheus.Registerer, routerLen func() float64) *selfMetrics {
	const ns = "pipeline"
	m := &selfMetrics{
		received: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "input_metrics_total",
			Help: "Metrics received from each input."}, []string{"input"}),
		processed: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "router_metrics_total",
			Help: "Metrics that passed through the processor stage."}),
		filtered: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "router_filtered_total",
			Help: "Metrics removed by processors."}),
		routerQueue: prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: ns, Name: "router_queue_batches",
			Help: "Batches waiting in the router queue."}, routerLen),
		enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "output_enqueued_total",
			Help: "Metrics accepted into each output queue."}, []string{"output"}),
		written: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "output_written_total",
			Help: "Metrics successfully written by each output."}, []string{"output"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "output_dropped_total",
			Help: "Metrics dropped by each output, by reason (queue_full, write_failed)."}, []string{"output", "reason"}),
		writeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "output_write_errors_total",
			Help: "Failed write attempts (including ones later retried successfully)."}, []string{"output"}),
		writeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "output_write_duration_seconds",
			Help: "Latency of output writes.", Buckets: prometheus.ExponentialBuckets(0.001, 4, 8)}, []string{"output"}),
		batchSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "output_batch_size",
			Help: "Metrics per output write.", Buckets: prometheus.ExponentialBuckets(1, 4, 9)}, []string{"output"}),
	}
	reg.MustRegister(m.received, m.processed, m.filtered, m.routerQueue, m.enqueued, m.written,
		m.dropped, m.writeErrors, m.writeDuration, m.batchSize)
	return m
}
