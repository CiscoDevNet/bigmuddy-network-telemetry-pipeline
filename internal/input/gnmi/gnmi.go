// Package gnmi implements a gNMI dial-in Subscribe client: the pipeline
// connects to each device (or to a gNMI gateway) and holds a long-lived
// STREAM subscription, reconnecting with exponential backoff and jitter.
//
// gNMI is the vendor-neutral successor to the Cisco-specific dial-in and
// dial-out transports the original pipeline supported; it is implemented by
// Cisco IOS XR/NX-OS/IOS XE, Juniper Junos/Evolved, Arista EOS, Nokia SR OS /
// SR Linux, SONiC and others.
package gnmi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"time"

	gpb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/prometheus/client_golang/prometheus"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

func init() { pipeline.RegisterInput("gnmi", New) }

// Target is one device. It may be given as a plain "host:port" string.
type Target struct {
	Address string `yaml:"address"`
	Name    string `yaml:"name"` // used as the source tag; defaults to the host part of Address
}

// UnmarshalYAML accepts either a string or a mapping.
func (t *Target) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		t.Address = n.Value
		return nil
	}
	type plain Target
	return n.Decode((*plain)(t))
}

// Subscription is one path within the subscription list.
type Subscription struct {
	Name              string        `yaml:"name"`
	Path              string        `yaml:"path"`
	Origin            string        `yaml:"origin"`
	Mode              string        `yaml:"mode"` // sample (default), on_change, target_defined
	SampleInterval    time.Duration `yaml:"sample_interval"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	SuppressRedundant bool          `yaml:"suppress_redundant"`
}

// Config is the gnmi input's YAML.
type Config struct {
	Targets       []Target       `yaml:"targets"`
	Username      string         `yaml:"username"`
	Password      string         `yaml:"password"`
	PasswordFile  string         `yaml:"password_file"`
	TLS           tlsutil.Config `yaml:"tls"`
	Encoding      string         `yaml:"encoding"` // proto (default), json_ietf, json
	Target        string         `yaml:"target"`   // gNMI prefix target, for gateways
	UpdatesOnly   bool           `yaml:"updates_only"`
	Compression   string         `yaml:"compression"` // "" or gzip
	Subscriptions []Subscription `yaml:"subscriptions"`
	RedialMin     time.Duration  `yaml:"redial_min"`
	RedialMax     time.Duration  `yaml:"redial_max"`
	MaxMsgSize    int            `yaml:"max_message_size"`
}

// Input is a running gnmi input.
type Input struct {
	cfg      Config
	password string
	req      *gpb.SubscribeRequest
	subPaths [][]string // element names per subscription, for tagging
	dialOpts []grpc.DialOption
	log      *slog.Logger

	up            *prometheus.GaugeVec
	notifications *prometheus.CounterVec
	reconnects    *prometheus.CounterVec
	syncs         *prometheus.CounterVec
	unsupported   *prometheus.CounterVec
}

// New builds a gnmi input from configuration.
func New(p *config.Plugin, env pipeline.Env) (pipeline.Input, error) {
	var c Config
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	if len(c.Targets) == 0 {
		return nil, errors.New("at least one target is required")
	}
	if len(c.Subscriptions) == 0 {
		return nil, errors.New("at least one subscription is required")
	}
	if c.RedialMin <= 0 {
		c.RedialMin = time.Second
	}
	if c.RedialMax <= 0 {
		c.RedialMax = time.Minute
	}
	if c.MaxMsgSize <= 0 {
		c.MaxMsgSize = 64 << 20
	}
	for i := range c.Targets {
		t := &c.Targets[i]
		if t.Address == "" {
			return nil, fmt.Errorf("target %d: address is required", i)
		}
		if _, _, err := net.SplitHostPort(t.Address); err != nil {
			return nil, fmt.Errorf("target %q: %w", t.Address, err)
		}
		if t.Name == "" {
			t.Name, _, _ = net.SplitHostPort(t.Address)
		}
	}
	pw, err := config.Secret(c.Password, c.PasswordFile)
	if err != nil {
		return nil, fmt.Errorf("password: %w", err)
	}
	in := &Input{cfg: c, password: pw, log: env.Logger}
	if in.req, in.subPaths, err = buildRequest(c); err != nil {
		return nil, err
	}

	tc, err := c.TLS.Client()
	if err != nil {
		return nil, err
	}
	creds := insecure.NewCredentials()
	if tc != nil {
		creds = credentials.NewTLS(tc)
	} else if c.Username != "" {
		env.Logger.Warn("sending gNMI credentials without TLS; enable tls for production")
	}
	in.dialOpts = []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.MaxMsgSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
	}
	switch c.Compression {
	case "":
	case "gzip":
		in.dialOpts = append(in.dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	default:
		return nil, fmt.Errorf("unsupported compression %q", c.Compression)
	}

	in.up = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gnmi_target_up",
		Help: "1 when the gNMI subscription to the target is established."}, []string{"target"})
	in.notifications = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gnmi_notifications_total",
		Help: "gNMI notifications received."}, []string{"target"})
	in.reconnects = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gnmi_session_failures_total",
		Help: "gNMI sessions that ended with an error."}, []string{"target"})
	in.syncs = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gnmi_sync_responses_total",
		Help: "gNMI sync_response messages received (initial state complete)."}, []string{"target"})
	in.unsupported = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gnmi_unsupported_values_total",
		Help: "Updates whose value type cannot be decoded generically."}, []string{"target"})
	env.Registry.MustRegister(in.up, in.notifications, in.reconnects, in.syncs, in.unsupported)
	return in, nil
}

func buildRequest(c Config) (*gpb.SubscribeRequest, [][]string, error) {
	enc := gpb.Encoding_PROTO
	switch strings.ToLower(c.Encoding) {
	case "", "proto":
	case "json_ietf":
		enc = gpb.Encoding_JSON_IETF
	case "json":
		enc = gpb.Encoding_JSON
	case "ascii":
		enc = gpb.Encoding_ASCII
	default:
		return nil, nil, fmt.Errorf("unsupported encoding %q", c.Encoding)
	}
	list := &gpb.SubscriptionList{Mode: gpb.SubscriptionList_STREAM, Encoding: enc, UpdatesOnly: c.UpdatesOnly}
	if c.Target != "" {
		list.Prefix = &gpb.Path{Target: c.Target}
	}
	var names [][]string
	for i, s := range c.Subscriptions {
		p, err := ParsePath(s.Origin, s.Path)
		if err != nil {
			return nil, nil, fmt.Errorf("subscription %d: %w", i, err)
		}
		sub := &gpb.Subscription{Path: p, SampleInterval: uint64(s.SampleInterval.Nanoseconds()),
			HeartbeatInterval: uint64(s.HeartbeatInterval.Nanoseconds()), SuppressRedundant: s.SuppressRedundant}
		switch strings.ToLower(s.Mode) {
		case "", "sample":
			sub.Mode = gpb.SubscriptionMode_SAMPLE
			if s.SampleInterval <= 0 {
				return nil, nil, fmt.Errorf("subscription %q: sample_interval is required for mode sample", s.Path)
			}
		case "on_change":
			sub.Mode = gpb.SubscriptionMode_ON_CHANGE
		case "target_defined":
			sub.Mode = gpb.SubscriptionMode_TARGET_DEFINED
		default:
			return nil, nil, fmt.Errorf("subscription %q: unknown mode %q", s.Path, s.Mode)
		}
		list.Subscription = append(list.Subscription, sub)
		elems := make([]string, len(p.Elem))
		for j, e := range p.Elem {
			elems[j] = stripModule(e.Name)
		}
		names = append(names, elems)
	}
	return &gpb.SubscribeRequest{Request: &gpb.SubscribeRequest_Subscribe{Subscribe: list}}, names, nil
}

// Run subscribes to every target until ctx is cancelled.
func (in *Input) Run(ctx context.Context, sink pipeline.Sink) error {
	var wg sync.WaitGroup
	for _, t := range in.cfg.Targets {
		wg.Add(1)
		go func() { defer wg.Done(); in.runTarget(ctx, t, sink) }()
	}
	wg.Wait()
	return nil
}

func (in *Input) runTarget(ctx context.Context, t Target, sink pipeline.Sink) {
	log := in.log.With("target", t.Name, "address", t.Address)
	backoff := in.cfg.RedialMin
	for {
		start := time.Now()
		err := in.session(ctx, t, sink, log)
		in.up.WithLabelValues(t.Name).Set(0)
		if ctx.Err() != nil {
			return
		}
		in.reconnects.WithLabelValues(t.Name).Inc()
		if time.Since(start) > 2*in.cfg.RedialMax {
			backoff = in.cfg.RedialMin // the session was healthy for a while
		}
		// Full jitter avoids thundering-herd reconnects across a fleet.
		wait := time.Duration(rand.Int64N(int64(backoff))) + backoff/2
		log.Warn("gNMI session ended; redialing", "err", err, "in", wait.Round(time.Millisecond))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, in.cfg.RedialMax)
	}
}

func (in *Input) session(ctx context.Context, t Target, sink pipeline.Sink, log *slog.Logger) error {
	conn, err := grpc.NewClient(t.Address, in.dialOpts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if in.cfg.Username != "" {
		sctx = metadata.AppendToOutgoingContext(sctx, "username", in.cfg.Username, "password", in.password)
	}
	stream, err := gpb.NewGNMIClient(conn).Subscribe(sctx)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	if err := stream.Send(in.req); err != nil {
		return fmt.Errorf("send subscribe request: %w", err)
	}
	log.Info("gNMI subscription sent", "paths", len(in.req.GetSubscribe().GetSubscription()))
	upSet := false

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("target closed the stream")
			}
			return err
		}
		if !upSet {
			in.up.WithLabelValues(t.Name).Set(1)
			upSet = true
		}
		switch r := resp.GetResponse().(type) {
		case *gpb.SubscribeResponse_Update:
			in.notifications.WithLabelValues(t.Name).Inc()
			base := map[string]string{telemetry.TagSource: t.Name}
			if sub := in.subscriptionFor(r.Update); sub != "" {
				base[telemetry.TagSubscription] = sub
			}
			ms, unsupported := Convert(r.Update, base)
			if unsupported > 0 {
				in.unsupported.WithLabelValues(t.Name).Add(float64(unsupported))
			}
			if err := sink.Emit(ctx, ms); err != nil {
				return err
			}
		case *gpb.SubscribeResponse_SyncResponse:
			in.syncs.WithLabelValues(t.Name).Inc()
			log.Debug("gNMI initial sync complete")
		}
	}
}

// subscriptionFor returns the configured name of the subscription whose path
// is a prefix of the notification's path (keys ignored).
func (in *Input) subscriptionFor(n *gpb.Notification) string {
	if len(in.cfg.Subscriptions) == 1 {
		return in.cfg.Subscriptions[0].Name
	}
	var elems []string
	for _, e := range n.GetPrefix().GetElem() {
		elems = append(elems, stripModule(e.GetName()))
	}
	if u := n.GetUpdate(); len(u) > 0 {
		for _, e := range u[0].GetPath().GetElem() {
			elems = append(elems, stripModule(e.GetName()))
		}
	}
	best, bestLen := "", -1
	for i, sp := range in.subPaths {
		if len(sp) > len(elems) || len(sp) <= bestLen {
			continue
		}
		match := true
		for j := range sp {
			if sp[j] != elems[j] {
				match = false
				break
			}
		}
		if match {
			best, bestLen = in.cfg.Subscriptions[i].Name, len(sp)
		}
	}
	return best
}
