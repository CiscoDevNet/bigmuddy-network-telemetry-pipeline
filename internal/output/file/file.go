// Package file writes metrics as JSON lines to a file or stdout. Use it to
// inspect exactly what the pipeline produces (the 2016 "tap"/"dump" role).
package file

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func init() { pipeline.RegisterOutput("file", New) }

// Config is the file output's YAML.
type Config struct {
	Path string `yaml:"path"` // "-" or empty for stdout
}

// Output is the JSON-lines writer.
type Output struct {
	mu sync.Mutex
	w  *bufio.Writer
	c  io.Closer
}

type line struct {
	Name      string            `json:"name"`
	Tags      map[string]string `json:"tags"`
	Fields    map[string]any    `json:"fields"`
	Timestamp time.Time         `json:"timestamp"`
}

// New opens the destination.
func New(p *config.Plugin, _ pipeline.Env) (pipeline.Output, error) {
	var c Config
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	if c.Path == "" || c.Path == "-" {
		return &Output{w: bufio.NewWriter(os.Stdout)}, nil
	}
	f, err := os.OpenFile(c.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Output{w: bufio.NewWriter(f), c: f}, nil
}

// Write appends one JSON document per metric.
func (o *Output) Write(_ context.Context, batch []*telemetry.Metric) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	enc := json.NewEncoder(o.w)
	for _, m := range batch {
		if err := enc.Encode(line{m.Name, m.Tags, m.Fields, m.Timestamp}); err != nil {
			return err
		}
	}
	return o.w.Flush()
}

// Close flushes and closes the file.
func (o *Output) Close(context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	err := o.w.Flush()
	if o.c != nil {
		if cerr := o.c.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
