// Package ciscomdt receives Cisco Model-Driven Telemetry *dial-out* streams:
// devices connect to the pipeline and push data.
//
//   - transport grpc: the gRPCMdtDialout service (IOS XR, NX-OS, IOS XE)
//   - transport tcp:  12-byte "ST" header framing (IOS XR)
//   - transport udp:  ST header per datagram (IOS XR)
//
// For new deployments prefer the gnmi input (dial-in) or gNMI dial-out where
// the platform supports it; this input exists so existing IOS XR fleets
// configured for MDT dial-out keep working unchanged.
package ciscomdt

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	mdpb "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/gen/cisco/mdtdialout"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

func init() { pipeline.RegisterInput("cisco_mdt", New) }

// Config is the cisco_mdt input's YAML.
type Config struct {
	Transport   string         `yaml:"transport"` // grpc (default), tcp, udp
	Listen      string         `yaml:"listen"`
	TLS         tlsutil.Config `yaml:"tls"`
	MaxMsgSize  int            `yaml:"max_message_size"`
	IdleTimeout time.Duration  `yaml:"idle_timeout"` // tcp: close silent connections
}

// Input is a running cisco_mdt listener.
type Input struct {
	cfg  Config
	tls  *tls.Config
	log  *slog.Logger
	addr chan net.Addr // reports the bound address (tests use :0)

	conns prometheus.Gauge
	msgs  prometheus.Counter
	errs  *prometheus.CounterVec
}

// New builds the input from configuration.
func New(p *config.Plugin, env pipeline.Env) (pipeline.Input, error) {
	c := Config{Transport: "grpc"}
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	if c.Listen == "" {
		return nil, errors.New("listen is required")
	}
	if c.MaxMsgSize <= 0 {
		c.MaxMsgSize = 16 << 20
	}
	tc, err := c.TLS.Server()
	if err != nil {
		return nil, err
	}
	switch c.Transport {
	case "grpc", "tcp":
	case "udp":
		if tc != nil {
			return nil, errors.New("tls is not supported with udp transport")
		}
	default:
		return nil, fmt.Errorf("unknown transport %q (grpc, tcp, udp)", c.Transport)
	}
	in := &Input{cfg: c, tls: tc, log: env.Logger, addr: make(chan net.Addr, 1),
		conns: prometheus.NewGauge(prometheus.GaugeOpts{Name: "mdt_connections",
			Help: "Open dial-out sessions from devices."}),
		msgs: prometheus.NewCounter(prometheus.CounterOpts{Name: "mdt_messages_total",
			Help: "Telemetry messages received."}),
		errs: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "mdt_decode_errors_total",
			Help: "Messages that could not be decoded, by reason."}, []string{"reason"}),
	}
	env.Registry.MustRegister(in.conns, in.msgs, in.errs)
	return in, nil
}

// Run listens until ctx is cancelled.
func (in *Input) Run(ctx context.Context, sink pipeline.Sink) error {
	switch in.cfg.Transport {
	case "tcp":
		return in.runTCP(ctx, sink)
	case "udp":
		return in.runUDP(ctx, sink)
	default:
		return in.runGRPC(ctx, sink)
	}
}

func (in *Input) handle(ctx context.Context, sink pipeline.Sink, payload []byte, enc Encoding, peerAddr string) error {
	in.msgs.Inc()
	ms, err := Decode(payload, enc, peerAddr)
	if err != nil {
		reason := "malformed"
		if errors.Is(err, ErrCompactGPB) {
			reason = "compact_gpb"
		}
		in.errs.WithLabelValues(reason).Inc()
		in.log.Debug("decode failed", "peer", peerAddr, "err", err)
		return nil // one bad message must not tear down the session
	}
	return sink.Emit(ctx, ms)
}

// --- gRPC dial-out ------------------------------------------------------

type grpcServer struct {
	mdpb.UnimplementedGRPCMdtDialoutServer
	in   *Input
	ctx  context.Context
	sink pipeline.Sink
}

func (s *grpcServer) MdtDialout(stream grpc.BidiStreamingServer[mdpb.MdtDialoutArgs, mdpb.MdtDialoutArgs]) error {
	addr := "unknown"
	if p, ok := peer.FromContext(stream.Context()); ok {
		addr, _, _ = net.SplitHostPort(p.Addr.String())
	}
	s.in.conns.Inc()
	defer s.in.conns.Dec()
	log := s.in.log.With("peer", addr)
	log.Info("dial-out session established")
	for {
		args, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || stream.Context().Err() != nil {
				log.Info("dial-out session closed")
				return nil
			}
			log.Warn("dial-out session failed", "err", err)
			return err
		}
		if args.GetErrors() != "" {
			log.Warn("device reported error", "msg", args.GetErrors())
		}
		if len(args.GetData()) == 0 {
			continue
		}
		if err := s.in.handle(s.ctx, s.sink, args.GetData(), EncodingUnknown, addr); err != nil {
			return err
		}
	}
}

func (in *Input) runGRPC(ctx context.Context, sink pipeline.Sink) error {
	lis, err := net.Listen("tcp", in.cfg.Listen)
	if err != nil {
		return err
	}
	in.addr <- lis.Addr()
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(in.cfg.MaxMsgSize),
		// XR sends keepalives as frequently as every 10s; don't GOAWAY them.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	}
	if in.tls != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(in.tls)))
	}
	srv := grpc.NewServer(opts...)
	mdpb.RegisterGRPCMdtDialoutServer(srv, &grpcServer{in: in, ctx: ctx, sink: sink})
	in.log.Info("listening for gRPC dial-out", "addr", lis.Addr().String(), "tls", in.tls != nil)

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(lis) }()
	select {
	case <-ctx.Done():
		stopped := make(chan struct{})
		go func() { srv.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			srv.Stop()
		}
		return nil
	case err := <-errc:
		return err
	}
}

// --- TCP / UDP with ST framing -----------------------------------------

const stHeaderLen = 12

const (
	stTypeData      = 1
	stTypeHeartbeat = 2

	stEncapGPB        = 1
	stEncapJSON       = 2
	stEncapGPBCompact = 3
	stEncapGPBKV      = 4
)

type stHeader struct {
	msgType, encap, version, flags uint16
	length                         uint32
}

func parseSTHeader(b []byte) stHeader {
	return stHeader{
		msgType: binary.BigEndian.Uint16(b[0:]),
		encap:   binary.BigEndian.Uint16(b[2:]),
		version: binary.BigEndian.Uint16(b[4:]),
		flags:   binary.BigEndian.Uint16(b[6:]),
		length:  binary.BigEndian.Uint32(b[8:]),
	}
}

func (h stHeader) validate(max int) error {
	switch {
	case h.version != 1:
		return fmt.Errorf("unsupported ST header version %d", h.version)
	case h.flags != 0:
		return fmt.Errorf("unsupported ST header flags %#x (compression?)", h.flags)
	case int64(h.length) > int64(max):
		return fmt.Errorf("message length %d exceeds max_message_size %d", h.length, max)
	}
	return nil
}

func (h stHeader) encoding() Encoding {
	switch h.encap {
	case stEncapJSON:
		return EncodingJSON
	case stEncapGPB, stEncapGPBCompact, stEncapGPBKV:
		return EncodingGPB
	}
	return EncodingUnknown
}

// EncodeST frames payload with an ST header (exported for tests and tools).
func EncodeST(payload []byte, json bool) []byte {
	b := make([]byte, stHeaderLen+len(payload))
	binary.BigEndian.PutUint16(b[0:], stTypeData)
	enc := uint16(stEncapGPB)
	if json {
		enc = stEncapJSON
	}
	binary.BigEndian.PutUint16(b[2:], enc)
	binary.BigEndian.PutUint16(b[4:], 1)
	binary.BigEndian.PutUint32(b[8:], uint32(len(payload)))
	copy(b[stHeaderLen:], payload)
	return b
}

func (in *Input) runTCP(ctx context.Context, sink pipeline.Sink) error {
	var lis net.Listener
	var err error
	if in.tls != nil {
		lis, err = tls.Listen("tcp", in.cfg.Listen, in.tls)
	} else {
		lis, err = net.Listen("tcp", in.cfg.Listen)
	}
	if err != nil {
		return err
	}
	in.addr <- lis.Addr()
	in.log.Info("listening for TCP dial-out", "addr", lis.Addr().String(), "tls", in.tls != nil)

	var wg sync.WaitGroup
	var mu sync.Mutex
	open := map[net.Conn]struct{}{}
	go func() {
		<-ctx.Done()
		lis.Close()
		mu.Lock()
		for c := range open {
			c.Close()
		}
		mu.Unlock()
	}()
	for {
		conn, err := lis.Accept()
		if err != nil {
			if ctx.Err() != nil {
				wg.Wait()
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		mu.Lock()
		open[conn] = struct{}{}
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { mu.Lock(); delete(open, conn); mu.Unlock(); conn.Close() }()
			in.serveTCP(ctx, sink, conn)
		}()
	}
}

func (in *Input) serveTCP(ctx context.Context, sink pipeline.Sink, conn net.Conn) {
	addr, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	log := in.log.With("peer", addr)
	in.conns.Inc()
	defer in.conns.Dec()
	log.Info("TCP dial-out session established")
	hdr := make([]byte, stHeaderLen)
	var buf []byte
	for {
		if in.cfg.IdleTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(in.cfg.IdleTimeout))
		}
		if _, err := io.ReadFull(conn, hdr); err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				log.Warn("TCP session ended", "err", err)
			}
			return
		}
		h := parseSTHeader(hdr)
		if err := h.validate(in.cfg.MaxMsgSize); err != nil {
			// Framing is lost; the only safe recovery is to drop the session.
			in.errs.WithLabelValues("framing").Inc()
			log.Warn("closing session: bad ST header", "err", err)
			return
		}
		if cap(buf) < int(h.length) {
			buf = make([]byte, h.length)
		}
		buf = buf[:h.length]
		if _, err := io.ReadFull(conn, buf); err != nil {
			log.Warn("TCP session ended mid-message", "err", err)
			return
		}
		if h.msgType == stTypeHeartbeat {
			continue
		}
		// Decode synchronously: the next read reuses buf.
		if err := in.handle(ctx, sink, buf, h.encoding(), addr); err != nil {
			return
		}
	}
}

func (in *Input) runUDP(ctx context.Context, sink pipeline.Sink) error {
	pc, err := net.ListenPacket("udp", in.cfg.Listen)
	if err != nil {
		return err
	}
	in.addr <- pc.LocalAddr()
	in.log.Info("listening for UDP dial-out", "addr", pc.LocalAddr().String())
	go func() { <-ctx.Done(); pc.Close() }()
	buf := make([]byte, 65535)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if n < stHeaderLen {
			in.errs.WithLabelValues("framing").Inc()
			continue
		}
		h := parseSTHeader(buf)
		if err := h.validate(in.cfg.MaxMsgSize); err != nil || int(h.length) != n-stHeaderLen {
			in.errs.WithLabelValues("framing").Inc()
			continue
		}
		if h.msgType == stTypeHeartbeat {
			continue
		}
		addr, _, _ := net.SplitHostPort(from.String())
		if err := in.handle(ctx, sink, buf[stHeaderLen:n], h.encoding(), addr); err != nil {
			return nil
		}
	}
}
