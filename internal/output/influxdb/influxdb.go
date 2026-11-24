// Package influxdb writes line protocol over HTTP.
//
// It speaks the /api/v2/write API (InfluxDB 2.x, InfluxDB 3 / Cloud, and
// most line-protocol-compatible TSDBs such as VictoriaMetrics, QuestDB and
// GreptimeDB) with token auth, and the legacy /write API for InfluxDB 1.x.
// The 2016 pipeline depended on the now-archived influxdb 1.x Go client.
package influxdb

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

func init() { pipeline.RegisterOutput("influxdb", New) }

// Config is the influxdb output's YAML.
type Config struct {
	URL string `yaml:"url"`
	// v2 / v3
	Org       string `yaml:"org"`
	Bucket    string `yaml:"bucket"`
	Token     string `yaml:"token"`
	TokenFile string `yaml:"token_file"`
	// v1
	Database        string `yaml:"database"`
	RetentionPolicy string `yaml:"retention_policy"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	PasswordFile    string `yaml:"password_file"`

	Gzip *bool          `yaml:"gzip"`
	TLS  tlsutil.Config `yaml:"tls"`
}

// Output is the InfluxDB writer.
type Output struct {
	cfg      Config
	endpoint string
	auth     func(*http.Request)
	gzip     bool
	v1       bool
	client   *http.Client
	log      *slog.Logger
}

// New builds the writer.
func New(p *config.Plugin, env pipeline.Env) (pipeline.Output, error) {
	var c Config
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	if c.URL == "" {
		return nil, errors.New("url is required")
	}
	base, err := url.Parse(strings.TrimRight(c.URL, "/"))
	if err != nil {
		return nil, err
	}
	o := &Output{cfg: c, gzip: c.Gzip == nil || *c.Gzip, log: env.Logger}
	q := url.Values{"precision": {"ns"}}
	switch {
	case c.Bucket != "" && c.Database != "":
		return nil, errors.New("set bucket (v2/v3) or database (v1), not both")
	case c.Bucket != "":
		token, err := config.Secret(c.Token, c.TokenFile)
		if err != nil {
			return nil, fmt.Errorf("token: %w", err)
		}
		q.Set("bucket", c.Bucket)
		if c.Org != "" {
			q.Set("org", c.Org)
		}
		base.Path += "/api/v2/write"
		o.auth = func(r *http.Request) {
			if token != "" {
				r.Header.Set("Authorization", "Token "+token)
			}
		}
	case c.Database != "":
		pw, err := config.Secret(c.Password, c.PasswordFile)
		if err != nil {
			return nil, fmt.Errorf("password: %w", err)
		}
		o.v1 = true
		q.Set("db", c.Database)
		if c.RetentionPolicy != "" {
			q.Set("rp", c.RetentionPolicy)
		}
		base.Path += "/write"
		o.auth = func(r *http.Request) {
			if c.Username != "" {
				r.SetBasicAuth(c.Username, pw)
			}
		}
	default:
		return nil, errors.New("bucket (InfluxDB 2.x/3.x) or database (InfluxDB 1.x) is required")
	}
	base.RawQuery = q.Encode()
	o.endpoint = base.String()
	tc, err := c.TLS.Client()
	if err != nil {
		return nil, err
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = tc
	o.client = &http.Client{Transport: tr}
	return o, nil
}

// Write encodes and posts a batch.
func (o *Output) Write(ctx context.Context, batch []*telemetry.Metric) error {
	var body bytes.Buffer
	for _, m := range batch {
		AppendLine(&body, m, o.v1)
	}
	if body.Len() == 0 {
		return nil
	}
	var rd io.Reader = &body
	if o.gzip {
		var zb bytes.Buffer
		zw := gzip.NewWriter(&zb)
		zw.Write(body.Bytes())
		zw.Close()
		rd = &zb
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.endpoint, rd)
	if err != nil {
		return pipeline.Permanent(err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if o.gzip {
		req.Header.Set("Content-Encoding", "gzip")
	}
	o.auth(req)
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	switch {
	case resp.StatusCode/100 == 2:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("influxdb HTTP %d: %s", resp.StatusCode, msg)
	default:
		// 400 (bad points), 401/403 (auth), 404 (bucket) won't fix themselves.
		return pipeline.Permanent(fmt.Errorf("influxdb HTTP %d: %s", resp.StatusCode, msg))
	}
}

// Close is a no-op.
func (o *Output) Close(context.Context) error { return nil }

var (
	measEsc = strings.NewReplacer(",", `\,`, " ", `\ `, "\n", `\n`)
	keyEsc  = strings.NewReplacer(",", `\,`, "=", `\=`, " ", `\ `, "\n", `\n`)
	strEsc  = strings.NewReplacer(`\`, `\\`, `"`, `\"`)
)

// AppendLine appends one line-protocol point for m. Points with no
// representable fields are skipped. With v1, unsigned integers are written as
// signed (InfluxDB 1.x OSS rejects the "u" suffix).
func AppendLine(b *bytes.Buffer, m *telemetry.Metric, v1 bool) {
	start := b.Len()
	b.WriteString(measEsc.Replace(m.Name))
	for _, k := range m.SortedTagKeys() {
		v := m.Tags[k]
		if v == "" || k == "" {
			continue
		}
		b.WriteByte(',')
		b.WriteString(keyEsc.Replace(k))
		b.WriteByte('=')
		b.WriteString(keyEsc.Replace(v))
	}
	sep := byte(' ')
	n := 0
	for _, k := range m.SortedFieldKeys() {
		var val string
		switch v := m.Fields[k].(type) {
		case int64:
			val = strconv.FormatInt(v, 10) + "i"
		case uint64:
			switch {
			case !v1:
				val = strconv.FormatUint(v, 10) + "u"
			case v <= math.MaxInt64:
				val = strconv.FormatUint(v, 10) + "i"
			default:
				val = strconv.FormatFloat(float64(v), 'g', -1, 64)
			}
		case float64:
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			val = strconv.FormatFloat(v, 'g', -1, 64)
		case bool:
			val = strconv.FormatBool(v)
		case string:
			val = `"` + strEsc.Replace(v) + `"`
		default:
			continue
		}
		b.WriteByte(sep)
		sep = ','
		b.WriteString(keyEsc.Replace(k))
		b.WriteByte('=')
		b.WriteString(val)
		n++
	}
	if n == 0 {
		b.Truncate(start)
		return
	}
	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(m.Timestamp.UnixNano(), 10))
	b.WriteByte('\n')
}
