// Package config loads the pipeline's YAML configuration.
//
// Design notes compared with the 2016 INI format:
//   - Secrets never live in the file. Use ${ENV_VAR} expansion or the
//     *_file options (Kubernetes/Docker secrets) instead of RSA-encrypted
//     passwords and interactive prompts.
//   - Every plugin is decoded strictly: an unknown or misspelt key is an
//     error at start-up rather than a silently ignored setting.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config is the root of the configuration file.
type Config struct {
	Log        Log      `yaml:"log"`
	Admin      Admin    `yaml:"admin"`
	Router     Router   `yaml:"router"`
	Inputs     []Plugin `yaml:"inputs"`
	Processors []Plugin `yaml:"processors"`
	Outputs    []Plugin `yaml:"outputs"`
}

// Log configures structured logging.
type Log struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // json or text
}

// Admin configures the self-monitoring HTTP endpoint.
type Admin struct {
	Listen string `yaml:"listen"` // ":9180"; empty disables
	Pprof  bool   `yaml:"pprof"`
}

// Router configures the central fan-out stage.
type Router struct {
	// QueueSize is the number of batches buffered between inputs and the
	// processor stage. When full, inputs block (gRPC/TCP flow control then
	// pushes back to the device rather than silently losing data).
	QueueSize int `yaml:"queue_size"`
}

// Plugin is one input, processor or output section. Type-specific settings
// are kept as a raw YAML node and decoded by the plugin itself.
type Plugin struct {
	Name string
	Type string
	// Output delivery settings (ignored for inputs/processors).
	Delivery Delivery
	settings yaml.Node
	line     int
}

// Delivery controls the per-output queue/batching/retry worker.
type Delivery struct {
	QueueSize     int           `yaml:"queue_size"`     // metrics buffered for this output
	BatchSize     int           `yaml:"batch_size"`     // max metrics per Write
	FlushInterval time.Duration `yaml:"flush_interval"` // max wait before a partial batch is written
	MaxRetries    int           `yaml:"max_retries"`    // per batch, on transient errors
	WriteTimeout  time.Duration `yaml:"write_timeout"`
}

var deliveryKeys = map[string]bool{"queue_size": true, "batch_size": true,
	"flush_interval": true, "max_retries": true, "write_timeout": true}

// UnmarshalYAML captures name/type and keeps the remaining keys for the
// plugin to decode strictly later.
func (p *Plugin) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: plugin section must be a mapping", n.Line)
	}
	p.line = n.Line
	rest := yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Line: n.Line}
	delivery := yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Line: n.Line}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		switch {
		case k.Value == "name":
			p.Name = v.Value
		case k.Value == "type":
			p.Type = v.Value
		case deliveryKeys[k.Value]:
			delivery.Content = append(delivery.Content, k, v)
		default:
			rest.Content = append(rest.Content, k, v)
		}
	}
	if p.Type == "" {
		return fmt.Errorf("line %d: plugin is missing 'type'", n.Line)
	}
	if len(delivery.Content) > 0 {
		if err := delivery.Decode(&p.Delivery); err != nil {
			return fmt.Errorf("line %d: %w", n.Line, err)
		}
	}
	p.settings = rest
	return nil
}

// Decode strictly decodes the plugin-specific settings into v. Unknown keys
// are rejected.
func (p *Plugin) Decode(v any) error {
	if len(p.settings.Content) == 0 {
		return nil
	}
	// Round-trip through bytes so we can use KnownFields.
	raw, err := yaml.Marshal(&p.settings)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%s %q (line %d): %w", p.Type, p.Name, p.line, err)
	}
	return nil
}

// HasDelivery reports whether any delivery keys were set.
func (p *Plugin) hasDelivery() bool { return p.Delivery != (Delivery{}) }

// Load reads, expands and parses a configuration file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

// Parse parses raw and expands environment variables in it.
//
// Expansion happens on parsed scalar values only, never on the raw text, so
// "${VAR}" inside comments is ignored and a variable's value can never inject
// YAML structure (a password containing ": " or newlines stays a string).
func Parse(raw []byte) (*Config, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	var missing []string
	expandNode(&root, &missing)
	if len(missing) > 0 {
		return nil, fmt.Errorf("config references unset environment variables: %s", strings.Join(missing, ", "))
	}
	expanded, err := yaml.Marshal(&root)
	if err != nil {
		return nil, err
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func expandNode(n *yaml.Node, missing *[]string) {
	if n.Kind == yaml.ScalarNode && strings.Contains(n.Value, "$") {
		v, err := ExpandEnv(n.Value, os.LookupEnv)
		if err != nil {
			var me missingEnvError
			if errors.As(err, &me) {
				*missing = append(*missing, me...)
			}
			return
		}
		if v != n.Value {
			n.Value = v
			if n.Style == 0 {
				n.Tag = "" // unquoted: let "${PORT}" resolve to an int, etc.
			} else {
				n.Tag = "!!str"
			}
		}
	}
	for _, c := range n.Content {
		expandNode(c, missing)
	}
}

type missingEnvError []string

func (m missingEnvError) Error() string {
	return "config references unset environment variables: " + strings.Join(m, ", ")
}

func (c *Config) applyDefaults() {
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
	if c.Router.QueueSize <= 0 {
		c.Router.QueueSize = 1024
	}
	name := func(ps []Plugin) {
		for i := range ps {
			if ps[i].Name == "" {
				ps[i].Name = fmt.Sprintf("%s-%d", ps[i].Type, i)
			}
		}
	}
	name(c.Inputs)
	name(c.Processors)
	name(c.Outputs)
	for i := range c.Outputs {
		d := &c.Outputs[i].Delivery
		if d.QueueSize <= 0 {
			d.QueueSize = 100_000
		}
		if d.BatchSize <= 0 {
			d.BatchSize = 1000
		}
		if d.FlushInterval <= 0 {
			d.FlushInterval = time.Second
		}
		if d.MaxRetries < 0 {
			d.MaxRetries = 0
		} else if d.MaxRetries == 0 {
			d.MaxRetries = 3
		}
		if d.WriteTimeout <= 0 {
			d.WriteTimeout = 10 * time.Second
		}
	}
}

func (c *Config) validate() error {
	var errs []error
	if len(c.Inputs) == 0 {
		errs = append(errs, errors.New("at least one input is required"))
	}
	if len(c.Outputs) == 0 {
		errs = append(errs, errors.New("at least one output is required"))
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		errs = append(errs, fmt.Errorf("log.format must be json or text, got %q", c.Log.Format))
	}
	seen := map[string]bool{}
	check := func(kind string, ps []Plugin, allowDelivery bool) {
		for _, p := range ps {
			key := kind + "/" + p.Name
			if seen[key] {
				errs = append(errs, fmt.Errorf("duplicate %s name %q", kind, p.Name))
			}
			seen[key] = true
			if !allowDelivery && p.hasDelivery() {
				errs = append(errs, fmt.Errorf("%s %q: delivery settings (queue_size, batch_size, ...) are only valid on outputs", kind, p.Name))
			}
		}
	}
	check("input", c.Inputs, false)
	check("processor", c.Processors, false)
	check("output", c.Outputs, true)
	return errors.Join(errs...)
}

var envRe = regexp.MustCompile(`\$\$|\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// ExpandEnv replaces ${VAR} and ${VAR:-default}. "$$" is a literal "$".
// A referenced variable that is unset and has no default is an error, so a
// missing secret is caught at start-up rather than sent as an empty string.
func ExpandEnv(s string, lookup func(string) (string, bool)) (string, error) {
	var missing []string
	out := envRe.ReplaceAllStringFunc(s, func(m string) string {
		if m == "$$" {
			return "$"
		}
		sub := envRe.FindStringSubmatch(m)
		if v, ok := lookup(sub[1]); ok {
			return v
		}
		if strings.Contains(m, ":-") {
			return sub[2]
		}
		missing = append(missing, sub[1])
		return ""
	})
	if len(missing) > 0 {
		return "", missingEnvError(missing)
	}
	return out, nil
}

// Secret resolves a secret given inline (already env-expanded) or via a file
// path. Trailing newlines in files are trimmed.
func Secret(inline, file string) (string, error) {
	if file == "" {
		return inline, nil
	}
	if inline != "" {
		return "", errors.New("set either the value or the _file variant, not both")
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}
