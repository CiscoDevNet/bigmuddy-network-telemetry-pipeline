// Package telemetry defines the vendor-neutral data model that flows through
// the pipeline. Every input (gNMI, Cisco MDT, ...) normalises what it receives
// into Metrics, and every output consumes Metrics. Nothing downstream of an
// input needs to know which device, vendor, transport or encoding produced
// the data.
package telemetry

import (
	"sort"
	"strings"
	"time"
)

// Well-known tag keys set by inputs.
const (
	TagSource       = "source"       // device that produced the data
	TagSubscription = "subscription" // subscription / sensor-group name
)

// Metric is a group of leaf values that share a path, a set of list keys and
// a timestamp. It maps 1:1 onto an InfluxDB point, and 1:N onto Prometheus /
// OTLP series (one per field).
//
// Metrics are treated as immutable once they leave the processor stage: the
// same *Metric is handed to every output concurrently.
type Metric struct {
	// Name is the schema path with list keys removed, e.g.
	// "openconfig:/interfaces/interface" or
	// "Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters".
	Name string `json:"name"`
	// Tags identify the instance: list keys, source, subscription, plus
	// anything added by processors.
	Tags map[string]string `json:"tags"`
	// Fields are leaf values keyed by path relative to Name. Values are one
	// of int64, uint64, float64, bool or string.
	Fields map[string]any `json:"fields"`
	// Timestamp is when the device sampled the data.
	Timestamp time.Time `json:"timestamp"`
}

// New returns an empty metric with initialised maps.
func New(name string, ts time.Time) *Metric {
	return &Metric{Name: name, Tags: map[string]string{}, Fields: map[string]any{}, Timestamp: ts}
}

// Source is shorthand for the source tag.
func (m *Metric) Source() string { return m.Tags[TagSource] }

// Clone returns a deep copy of the metric.
func (m *Metric) Clone() *Metric {
	c := &Metric{Name: m.Name, Timestamp: m.Timestamp,
		Tags: make(map[string]string, len(m.Tags)), Fields: make(map[string]any, len(m.Fields))}
	for k, v := range m.Tags {
		c.Tags[k] = v
	}
	for k, v := range m.Fields {
		c.Fields[k] = v
	}
	return c
}

// SortedTagKeys returns tag keys in lexical order (for deterministic output).
func (m *Metric) SortedTagKeys() []string { return sortedKeys(m.Tags) }

// SortedFieldKeys returns field keys in lexical order.
func (m *Metric) SortedFieldKeys() []string { return sortedKeys(m.Fields) }

func sortedKeys[V any](mp map[string]V) []string {
	keys := make([]string, 0, len(mp))
	for k := range mp {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// SeriesKey returns a stable identity for the metric's name+tags.
func (m *Metric) SeriesKey() string {
	var b strings.Builder
	b.WriteString(m.Name)
	for _, k := range m.SortedTagKeys() {
		b.WriteByte(0)
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(m.Tags[k])
	}
	return b.String()
}

// ToFloat converts a numeric or boolean field value to float64. ok is false
// for strings and unknown types.
func ToFloat(v any) (f float64, ok bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case uint32:
		return float64(x), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// Normalize converts a value of any supported Go scalar type to one of the
// canonical field types (int64, uint64, float64, bool, string). ok is false
// if the type is not representable.
func Normalize(v any) (any, bool) {
	switch x := v.(type) {
	case int64, uint64, float64, bool, string:
		return x, true
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	case uint:
		return uint64(x), true
	case uint32:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint8:
		return uint64(x), true
	case float32:
		return float64(x), true
	case []byte:
		return string(x), true
	}
	return nil, false
}
