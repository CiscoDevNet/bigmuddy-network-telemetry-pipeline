package ciscomdt

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	mdpb "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/gen/cisco/mdtdialout"
	tpb "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/gen/cisco/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name + ".kvgpb")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Fixtures are real IOS XR KV-GPB messages captured by the 2016 pipeline.
func TestDecodeRealKVGPB(t *testing.T) {
	ms, err := Decode(fixture(t, "generic_counters"), EncodingGPB, "10.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 8 {
		t.Fatalf("expected 8 interfaces, got %d", len(ms))
	}
	m := ms[0]
	if m.Name != "Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters" {
		t.Errorf("name %q", m.Name)
	}
	if m.Source() != "dr02" || m.Tags["interface-name"] == "" || m.Tags[telemetry.TagSubscription] == "" {
		t.Errorf("tags %v", m.Tags)
	}
	if _, ok := m.Fields["bytes-received"].(uint64); !ok {
		t.Errorf("bytes-received missing or wrong type: %#v", m.Fields)
	}
	if m.Timestamp.Year() != 2016 {
		t.Errorf("timestamp %v", m.Timestamp)
	}

	ms, err = Decode(fixture(t, "memory_summary"), EncodingUnknown, "")
	if err != nil || len(ms) != 4 || ms[0].Tags["node-name"] == "" {
		t.Fatalf("memory summary: %v %+v", err, ms)
	}

	// Deeply nested BGP data flattens into path-named fields.
	ms, err = Decode(fixture(t, "bgp_process_info"), EncodingGPB, "")
	if err != nil || len(ms) != 1 {
		t.Fatalf("bgp: %v", err)
	}
	var nested int
	for k := range ms[0].Fields {
		if strings.Count(k, "/") >= 1 {
			nested++
		}
	}
	if nested == 0 || len(ms[0].Fields) < 50 {
		t.Errorf("expected many nested fields, got %d (%d nested)", len(ms[0].Fields), nested)
	}
}

func TestDecodeRepeatedSiblingsAndCompact(t *testing.T) {
	msg := &tpb.Telemetry{
		EncodingPath: "p",
		DataGpbkv: []*tpb.TelemetryField{{Fields: []*tpb.TelemetryField{
			{Name: "keys", Fields: []*tpb.TelemetryField{{Name: "k", ValueByType: &tpb.TelemetryField_Uint32Value{Uint32Value: 7}}}},
			{Name: "content", Fields: []*tpb.TelemetryField{
				{Name: "q", Fields: []*tpb.TelemetryField{{Name: "v", ValueByType: &tpb.TelemetryField_Sint64Value{Sint64Value: -1}}}},
				{Name: "q", Fields: []*tpb.TelemetryField{{Name: "v", ValueByType: &tpb.TelemetryField_DoubleValue{DoubleValue: 2.5}}}},
			}},
		}}},
	}
	b, _ := proto.Marshal(msg)
	ms, err := Decode(b, EncodingGPB, "1.2.3.4")
	if err != nil || len(ms) != 1 {
		t.Fatal(err)
	}
	if ms[0].Fields["q/0/v"] != int64(-1) || ms[0].Fields["q/1/v"] != 2.5 || ms[0].Tags["k"] != "7" || ms[0].Source() != "1.2.3.4" {
		t.Errorf("got %+v", ms[0])
	}

	compact, _ := proto.Marshal(&tpb.Telemetry{DataGpb: &tpb.TelemetryGPBTable{Row: []*tpb.TelemetryRowGPB{{Content: []byte{1}}}}})
	if _, err := Decode(compact, EncodingGPB, ""); err != ErrCompactGPB {
		t.Errorf("expected ErrCompactGPB, got %v", err)
	}
}

const xrJSON = `{"node_id_str":"r9","subscription_id_str":"s","encoding_path":"Cisco-IOS-XR-nto-misc-oper:memory-summary/nodes/node/summary",
"msg_timestamp":1700000000000,"data_json":[{"timestamp":1700000000123,"keys":{"node-name":"0/RP0/CPU0"},
"content":{"page-size":4096,"ram-memory":"8589934592","free-physical-memory":12345,"boot":{"ok":true}}}]}`

func TestDecodeJSON(t *testing.T) {
	ms, err := Decode([]byte(xrJSON), EncodingUnknown, "")
	if err != nil || len(ms) != 1 {
		t.Fatal(err)
	}
	m := ms[0]
	if m.Source() != "r9" || m.Tags["node-name"] != "0/RP0/CPU0" || m.Fields["page-size"] != int64(4096) ||
		m.Fields["boot/ok"] != true || m.Timestamp.UnixMilli() != 1700000000123 {
		t.Errorf("got %+v", m)
	}
	// keys as a list of objects
	ms, err = Decode([]byte(`{"encoding_path":"p","data_json":[{"keys":[{"a":"1"},{"b":2}],"content":{"x":1}}]}`), EncodingJSON, "peer")
	if err != nil || ms[0].Tags["a"] != "1" || ms[0].Tags["b"] != "2" || ms[0].Source() != "peer" {
		t.Errorf("list keys: %v %+v", err, ms)
	}
}

// --- transport tests ----------------------------------------------------

type chanSink chan *telemetry.Metric

func (c chanSink) Emit(ctx context.Context, b []*telemetry.Metric) error {
	for _, m := range b {
		c <- m
	}
	return nil
}

func start(t *testing.T, transport string) (*Input, net.Addr, chanSink) {
	t.Helper()
	cfg, err := config.Parse([]byte(`
inputs: [{type: cisco_mdt, transport: ` + transport + `, listen: "127.0.0.1:0", max_message_size: 1048576}]
outputs: [{type: file}]`))
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(&cfg.Inputs[0], pipeline.Env{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Registry: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	in := p.(*Input)
	ctx, cancel := context.WithCancel(context.Background())
	sink := make(chanSink, 100)
	done := make(chan error, 1)
	go func() { done <- in.Run(ctx, sink) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})
	return in, <-in.addr, sink
}

func recv(t *testing.T, s chanSink, n int) []*telemetry.Metric {
	t.Helper()
	var out []*telemetry.Metric
	for len(out) < n {
		select {
		case m := <-s:
			out = append(out, m)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout: got %d/%d metrics", len(out), n)
		}
	}
	return out
}

func TestGRPCDialout(t *testing.T) {
	_, addr, sink := start(t, "grpc")
	conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := mdpb.NewGRPCMdtDialoutClient(conn).MdtDialout(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&mdpb.MdtDialoutArgs{ReqId: 1, Data: fixture(t, "generic_counters")}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&mdpb.MdtDialoutArgs{ReqId: 2, Data: []byte(xrJSON)}); err != nil {
		t.Fatal(err)
	}
	ms := recv(t, sink, 9)
	if ms[8].Source() != "r9" {
		t.Errorf("JSON over gRPC not decoded: %+v", ms[8])
	}
	stream.CloseSend()
}

func TestTCPDialout(t *testing.T) {
	in, addr, sink := start(t, "tcp")
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	// Garbage-but-well-framed message is counted, not fatal.
	conn.Write(EncodeST([]byte{0xff, 0xff}, false))
	conn.Write(EncodeST(fixture(t, "memory_summary"), false))
	conn.Write(EncodeST([]byte(xrJSON), true))
	ms := recv(t, sink, 5)
	if ms[4].Source() != "r9" || ms[0].Source() != "dr02" {
		t.Errorf("unexpected sources %s %s", ms[0].Source(), ms[4].Source())
	}
	if v := testutil.ToFloat64(in.errs.WithLabelValues("malformed")); v != 1 {
		t.Errorf("malformed count %v", v)
	}
	// Oversized frame closes the session.
	big := EncodeST(make([]byte, 10), false)
	big[8], big[9] = 0x7f, 0xff
	conn.Write(big[:12])
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Error("expected server to close connection after bad header")
	}
	conn.Close()
}

func TestUDPDialout(t *testing.T) {
	_, addr, sink := start(t, "udp")
	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write([]byte{1, 2, 3}) // runt
	conn.Write(EncodeST(fixture(t, "memory_summary"), false))
	recv(t, sink, 4)
}
