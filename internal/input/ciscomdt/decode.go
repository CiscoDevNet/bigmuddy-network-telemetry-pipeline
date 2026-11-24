package ciscomdt

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	tpb "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/gen/cisco/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// ErrCompactGPB is returned for compact (schema-specific) GPB payloads.
//
// The 2016 pipeline decoded compact GPB by compiling ~3,600 generated
// per-model .proto packages into the binary. That approach does not scale
// across vendors and software releases; self-describing encodings (KV-GPB,
// JSON, gNMI) carry the schema on the wire and are what modern deployments
// use. Configure the device with "encoding self-describing-gpb".
var ErrCompactGPB = errors.New("compact GPB is not supported; configure the device for self-describing-gpb (KV-GPB) or use gNMI")

// Encoding of an MDT payload.
type Encoding int

const (
	EncodingUnknown Encoding = iota
	EncodingGPB              // Telemetry message; KV or compact decided by content
	EncodingJSON
)

// Decode decodes one MDT payload into metrics. peer is used as the source
// when the payload does not carry a node id.
func Decode(payload []byte, enc Encoding, peer string) ([]*telemetry.Metric, error) {
	if enc == EncodingUnknown {
		enc = EncodingGPB
		if t := bytes.TrimLeft(payload, " \t\r\n"); len(t) > 0 && t[0] == '{' {
			enc = EncodingJSON
		}
	}
	if enc == EncodingJSON {
		return decodeJSON(payload, peer)
	}
	var t tpb.Telemetry
	if err := proto.Unmarshal(payload, &t); err != nil {
		return nil, fmt.Errorf("unmarshal telemetry: %w", err)
	}
	return FromTelemetry(&t, peer)
}

// FromTelemetry converts a decoded KV-GPB Telemetry message.
func FromTelemetry(t *tpb.Telemetry, peer string) ([]*telemetry.Metric, error) {
	if len(t.GetDataGpb().GetRow()) > 0 {
		return nil, ErrCompactGPB
	}
	base := baseTags(t.GetNodeIdStr(), t.GetSubscriptionIdStr(), peer)
	msgTS := msToTime(t.GetMsgTimestamp())
	out := make([]*telemetry.Metric, 0, len(t.GetDataGpbkv()))
	for _, row := range t.GetDataGpbkv() {
		ts := msgTS
		if row.GetTimestamp() != 0 {
			ts = msToTime(row.GetTimestamp())
		}
		m := telemetry.New(t.GetEncodingPath(), ts)
		for k, v := range base {
			m.Tags[k] = v
		}
		for _, f := range row.GetFields() {
			switch f.GetName() {
			case "keys":
				leaves := map[string]any{}
				flattenKV("", f.GetFields(), leaves)
				for k, v := range leaves {
					m.Tags[k] = fmt.Sprint(v)
				}
			case "content":
				flattenKV("", f.GetFields(), m.Fields)
			default:
				// Some event-driven XR sensors put leaves at the row level.
				flattenKV("", []*tpb.TelemetryField{f}, m.Fields)
			}
		}
		if len(m.Fields) > 0 {
			out = append(out, m)
		}
	}
	return out, nil
}

// flattenKV flattens nested KV-GPB fields. Repeated siblings (YANG lists or
// leaf-lists inside the content) are disambiguated by index: "name/0", ...
func flattenKV(prefix string, fs []*tpb.TelemetryField, out map[string]any) {
	counts := make(map[string]int, len(fs))
	for _, f := range fs {
		counts[f.GetName()]++
	}
	seen := make(map[string]int, len(fs))
	for _, f := range fs {
		name := telemetry.JoinPath(prefix, f.GetName())
		if counts[f.GetName()] > 1 {
			name = telemetry.JoinPath(name, strconv.Itoa(seen[f.GetName()]))
			seen[f.GetName()]++
		}
		if len(f.GetFields()) > 0 {
			flattenKV(name, f.GetFields(), out)
			continue
		}
		if v, ok := kvValue(f); ok {
			out[name] = v
		}
	}
}

func kvValue(f *tpb.TelemetryField) (any, bool) {
	switch v := f.GetValueByType().(type) {
	case *tpb.TelemetryField_BytesValue:
		return base64.StdEncoding.EncodeToString(v.BytesValue), true
	case *tpb.TelemetryField_StringValue:
		return v.StringValue, true
	case *tpb.TelemetryField_BoolValue:
		return v.BoolValue, true
	case *tpb.TelemetryField_Uint32Value:
		return uint64(v.Uint32Value), true
	case *tpb.TelemetryField_Uint64Value:
		return v.Uint64Value, true
	case *tpb.TelemetryField_Sint32Value:
		return int64(v.Sint32Value), true
	case *tpb.TelemetryField_Sint64Value:
		return v.Sint64Value, true
	case *tpb.TelemetryField_DoubleValue:
		return v.DoubleValue, true
	case *tpb.TelemetryField_FloatValue:
		return float64(v.FloatValue), true
	}
	return nil, false
}

// jsonMsg is the IOS XR JSON telemetry envelope.
type jsonMsg struct {
	NodeID       string          `json:"node_id_str"`
	Subscription string          `json:"subscription_id_str"`
	EncodingPath string          `json:"encoding_path"`
	MsgTimestamp json.Number     `json:"msg_timestamp"`
	Data         []jsonRow       `json:"data_json"`
	DataGPB      json.RawMessage `json:"data_gpb"`
}

type jsonRow struct {
	Timestamp json.Number `json:"timestamp"`
	Keys      any         `json:"keys"`
	Content   any         `json:"content"`
}

func decodeJSON(payload []byte, peer string) ([]*telemetry.Metric, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var msg jsonMsg
	if err := dec.Decode(&msg); err != nil {
		return nil, fmt.Errorf("unmarshal JSON telemetry: %w", err)
	}
	base := baseTags(msg.NodeID, msg.Subscription, peer)
	msgTS := msToTime(numToUint(msg.MsgTimestamp))
	out := make([]*telemetry.Metric, 0, len(msg.Data))
	for _, row := range msg.Data {
		ts := msgTS
		if t := numToUint(row.Timestamp); t != 0 {
			ts = msToTime(t)
		}
		m := telemetry.New(msg.EncodingPath, ts)
		for k, v := range base {
			m.Tags[k] = v
		}
		// keys is an object on most releases and a list of objects on some.
		keyLeaves := map[string]any{}
		telemetry.FlattenJSON("", row.Keys, keyLeaves)
		if _, isList := row.Keys.([]any); isList {
			keyLeaves = map[string]any{}
			for _, k := range row.Keys.([]any) {
				telemetry.FlattenJSON("", k, keyLeaves)
			}
		}
		for k, v := range keyLeaves {
			m.Tags[k] = fmt.Sprint(v)
		}
		telemetry.FlattenJSON("", row.Content, m.Fields)
		if len(m.Fields) > 0 {
			out = append(out, m)
		}
	}
	return out, nil
}

func baseTags(node, sub, peer string) map[string]string {
	tags := map[string]string{telemetry.TagSource: node}
	if node == "" {
		tags[telemetry.TagSource] = peer
	}
	if sub != "" {
		tags[telemetry.TagSubscription] = sub
	}
	return tags
}

func numToUint(n json.Number) uint64 {
	u, _ := strconv.ParseUint(string(n), 10, 64)
	return u
}

func msToTime(ms uint64) time.Time {
	if ms == 0 {
		return time.Now()
	}
	return time.UnixMilli(int64(ms))
}
