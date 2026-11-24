package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExpandEnv(t *testing.T) {
	env := map[string]string{"A": "alpha", "EMPTY": ""}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }

	got, err := ExpandEnv("x=${A} y=${B:-dflt} z=${EMPTY:-no} $$HOME", lookup)
	if err != nil {
		t.Fatal(err)
	}
	if want := "x=alpha y=dflt z= $HOME"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if _, err := ExpandEnv("${MISSING} ${ALSO}", lookup); err == nil ||
		!strings.Contains(err.Error(), "MISSING, ALSO") {
		t.Fatalf("expected missing var error, got %v", err)
	}
}

const sample = `
log: {level: debug, format: text}
inputs:
  - type: gnmi
    targets: [a]
outputs:
  - name: prom
    type: prometheus
    listen: ":0"
    batch_size: 10
    flush_interval: 2s
`

type fakeGNMI struct {
	Targets []string `yaml:"targets"`
}

func TestParseAndStrictDecode(t *testing.T) {
	c, err := Parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	if c.Inputs[0].Name != "gnmi-0" {
		t.Errorf("default name not applied: %q", c.Inputs[0].Name)
	}
	var g fakeGNMI
	if err := c.Inputs[0].Decode(&g); err != nil || len(g.Targets) != 1 {
		t.Fatalf("decode: %v %v", g, err)
	}
	d := c.Outputs[0].Delivery
	if d.BatchSize != 10 || d.FlushInterval != 2*time.Second || d.QueueSize != 100_000 || d.MaxRetries != 3 {
		t.Errorf("unexpected delivery %+v", d)
	}
	// Unknown key in plugin must be rejected.
	var strict struct {
		Targets []string `yaml:"targets"`
	}
	c.Inputs[0].settings.Content = append(c.Inputs[0].settings.Content, c.Outputs[0].settings.Content...)
	if err := c.Inputs[0].Decode(&strict); err == nil {
		t.Error("expected unknown field error")
	}
}

func TestValidation(t *testing.T) {
	bad := []string{
		`outputs: [{type: file}]`, // no inputs
		`inputs: [{type: gnmi}]`,  // no outputs
		`inputs: [{type: gnmi, batch_size: 5}]
outputs: [{type: file}]`, // delivery on input
		`inputs: [{name: a, type: gnmi}, {name: a, type: gnmi}]
outputs: [{type: file}]`, // duplicate
		`bogus: 1
inputs: [{type: gnmi}]
outputs: [{type: file}]`, // unknown top-level key
	}
	for _, b := range bad {
		if _, err := Parse([]byte(b)); err == nil {
			t.Errorf("expected error for:\n%s", b)
		}
	}
}

func TestSecret(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "pw")
	if err := os.WriteFile(f, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if v, err := Secret("", f); err != nil || v != "s3cret" {
		t.Fatalf("got %q %v", v, err)
	}
	if v, _ := Secret("inline", ""); v != "inline" {
		t.Fatal("inline secret not returned")
	}
	if _, err := Secret("x", f); err == nil {
		t.Fatal("expected error when both set")
	}
}

func TestEnvOnlyInValues(t *testing.T) {
	t.Setenv("PW", "a: b\n- c")
	t.Setenv("QS", "77")
	c, err := Parse([]byte(`
# password: ${NOT_SET_BUT_IN_COMMENT}
router:
  queue_size: ${QS}
inputs:
  - type: gnmi
    password: ${PW}
    quoted: "${QS}"
outputs: [{type: file}]
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Router.QueueSize != 77 {
		t.Errorf("unquoted env not resolved as int: %d", c.Router.QueueSize)
	}
	var v struct {
		Password string `yaml:"password"`
		Quoted   string `yaml:"quoted"`
	}
	if err := c.Inputs[0].Decode(&v); err != nil {
		t.Fatal(err)
	}
	if v.Password != "a: b\n- c" || v.Quoted != "77" {
		t.Errorf("got %+v", v)
	}
	if _, err := Parse([]byte("inputs:\n  - type: x\n    p: ${NOPE_X}\noutputs: [{type: y}]")); err == nil ||
		!strings.Contains(err.Error(), "NOPE_X") {
		t.Errorf("missing var not reported: %v", err)
	}
}
