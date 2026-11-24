package gnmi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"sort"
	"strings"
	"time"

	gpb "github.com/openconfig/gnmi/proto/gnmi"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// Convert turns a gNMI Notification into Metrics.
//
// Updates are grouped by their *instance path*: the path up to and
// including the last list element that has keys. For
//
//	/interfaces/interface[name=Ethernet1]/state/counters/in-octets
//
// the metric name is "/interfaces/interface", the tag set is
// {interface_name: Ethernet1} and the field is "state/counters/in-octets".
// All leaves of one interface in a notification therefore land in a single
// Metric (one InfluxDB point), regardless of how the device split them.
//
// Tag keys are "<list>_<key>" with module prefixes removed, which keeps them
// unique across nested lists (interface_name, subinterface_index).
func Convert(n *gpb.Notification, base map[string]string) (ms []*telemetry.Metric, unsupported int) {
	ts := time.Unix(0, n.GetTimestamp())
	if n.GetTimestamp() == 0 {
		ts = time.Now()
	}
	prefix := n.GetPrefix()
	groups := map[string]*telemetry.Metric{}
	var order []string

	for _, u := range n.GetUpdate() {
		elems := append(append([]*gpb.PathElem{}, prefix.GetElem()...), u.GetPath().GetElem()...)
		origin := u.GetPath().GetOrigin()
		if origin == "" {
			origin = prefix.GetOrigin()
		}

		// Split at the last keyed element.
		split := -1
		for i, e := range elems {
			if len(e.GetKey()) > 0 {
				split = i
			}
		}
		var root, rel []*gpb.PathElem
		switch {
		case split >= 0:
			root, rel = elems[:split+1], elems[split+1:]
		case len(elems) > 0:
			root, rel = elems[:len(elems)-1], elems[len(elems)-1:]
		}

		name := pathString(origin, root)
		tags := make(map[string]string, len(base)+4)
		for k, v := range base {
			tags[k] = v
		}
		for _, e := range root {
			list := stripModule(e.GetName())
			for k, v := range e.GetKey() {
				tags[list+"_"+stripModule(k)] = v
			}
		}

		key := name + "\x00" + tagKey(tags)
		m, ok := groups[key]
		if !ok {
			m = &telemetry.Metric{Name: name, Tags: tags, Fields: map[string]any{}, Timestamp: ts}
			groups[key] = m
			order = append(order, key)
		}

		field := relString(rel)
		if !addValue(m.Fields, field, u.GetVal()) {
			unsupported++
		}
	}

	for _, k := range order {
		if m := groups[k]; len(m.Fields) > 0 {
			ms = append(ms, m)
		}
	}
	return ms, unsupported
}

// addValue decodes a TypedValue into fields. JSON values are flattened.
func addValue(fields map[string]any, field string, v *gpb.TypedValue) bool {
	if v == nil {
		return false
	}
	switch x := v.GetValue().(type) {
	case *gpb.TypedValue_IntVal:
		fields[leaf(field)] = x.IntVal
	case *gpb.TypedValue_UintVal:
		fields[leaf(field)] = x.UintVal
	case *gpb.TypedValue_FloatVal: //nolint:staticcheck // deprecated but still sent by some devices
		fields[leaf(field)] = float64(x.FloatVal)
	case *gpb.TypedValue_DoubleVal:
		fields[leaf(field)] = x.DoubleVal
	case *gpb.TypedValue_DecimalVal: //nolint:staticcheck // deprecated but still sent by some devices
		d := x.DecimalVal
		fields[leaf(field)] = float64(d.GetDigits()) / math.Pow10(int(d.GetPrecision()))
	case *gpb.TypedValue_BoolVal:
		fields[leaf(field)] = x.BoolVal
	case *gpb.TypedValue_StringVal:
		fields[leaf(field)] = x.StringVal
	case *gpb.TypedValue_AsciiVal:
		fields[leaf(field)] = x.AsciiVal
	case *gpb.TypedValue_BytesVal:
		fields[leaf(field)] = base64.StdEncoding.EncodeToString(x.BytesVal)
	case *gpb.TypedValue_JsonIetfVal:
		return addJSON(fields, field, x.JsonIetfVal)
	case *gpb.TypedValue_JsonVal:
		return addJSON(fields, field, x.JsonVal)
	case *gpb.TypedValue_LeaflistVal:
		parts := make([]string, 0, len(x.LeaflistVal.GetElement()))
		for _, el := range x.LeaflistVal.GetElement() {
			tmp := map[string]any{}
			if addValue(tmp, "v", el) {
				if s, ok := tmp["v"].(string); ok {
					parts = append(parts, s)
				} else {
					b, _ := json.Marshal(tmp["v"])
					parts = append(parts, string(b))
				}
			}
		}
		fields[leaf(field)] = strings.Join(parts, ",")
	default:
		return false // AnyVal, ProtoBytes: schema-specific, not decodable generically
	}
	return true
}

func addJSON(fields map[string]any, field string, raw []byte) bool {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return false
	}
	// RFC 7951 encodes 64-bit integers as strings; recover them as numbers.
	v = unquoteNumbers(v)
	telemetry.FlattenJSON(stripJSONModules(field), stripJSONModules(v), fields)
	return true
}

// stripJSONModules removes "module:" prefixes from RFC 7951 member names.
func stripJSONModules[T any](v T) T {
	switch x := any(v).(type) {
	case string:
		parts := strings.Split(x, "/")
		for i := range parts {
			parts[i] = stripModule(parts[i])
		}
		return any(strings.Join(parts, "/")).(T)
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, c := range x {
			out[stripModule(k)] = stripJSONModules(c)
		}
		return any(out).(T)
	case []any:
		for i := range x {
			x[i] = stripJSONModules(x[i])
		}
	}
	return v
}

func unquoteNumbers(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, c := range x {
			x[k] = unquoteNumbers(c)
		}
	case []any:
		for i := range x {
			x[i] = unquoteNumbers(x[i])
		}
	case string:
		if looksNumeric(x) {
			return json.Number(x)
		}
	}
	return v
}

func looksNumeric(s string) bool {
	if s == "" || len(s) > 24 {
		return false
	}
	for i, r := range s {
		if (r < '0' || r > '9') && !(i == 0 && r == '-') && r != '.' {
			return false
		}
	}
	return s != "-" && s != "."
}

func leaf(f string) string {
	if f == "" {
		return "value"
	}
	return f
}

func stripModule(s string) string {
	if i := strings.IndexByte(s, ':'); i >= 0 {
		return s[i+1:]
	}
	return s
}

func pathString(origin string, elems []*gpb.PathElem) string {
	var b strings.Builder
	if origin != "" {
		b.WriteString(origin)
		b.WriteByte(':')
	}
	for _, e := range elems {
		b.WriteByte('/')
		b.WriteString(stripModule(e.GetName()))
	}
	if len(elems) == 0 {
		b.WriteByte('/')
	}
	return b.String()
}

func relString(elems []*gpb.PathElem) string {
	parts := make([]string, len(elems))
	for i, e := range elems {
		parts[i] = stripModule(e.GetName())
	}
	return strings.Join(parts, "/")
}

func tagKey(tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(tags[k])
		b.WriteByte(0)
	}
	return b.String()
}

// ParsePath parses a gNMI path string such as
// "/interfaces/interface[name=Ethernet1]/state" into a gNMI Path. Key values
// may contain '/' and escaped ']' characters.
func ParsePath(origin, s string) (*gpb.Path, error) {
	p := &gpb.Path{Origin: origin}
	s = strings.TrimSpace(s)
	if i := strings.Index(s, ":/"); i > 0 && !strings.ContainsAny(s[:i], "/[") && origin == "" {
		p.Origin, s = s[:i], s[i+1:]
	}
	s = strings.Trim(s, "/")
	if s == "" {
		return p, nil
	}
	var elems []string
	var cur strings.Builder
	depth := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s):
			cur.WriteByte(c)
			cur.WriteByte(s[i+1])
			i++
			continue
		case c == '[':
			depth++
		case c == ']':
			depth--
		case c == '/' && depth == 0:
			elems = append(elems, cur.String())
			cur.Reset()
			continue
		}
		cur.WriteByte(c)
	}
	if depth != 0 {
		return nil, errBadPath(s)
	}
	elems = append(elems, cur.String())
	for _, e := range elems {
		pe, err := parseElem(e)
		if err != nil {
			return nil, err
		}
		p.Elem = append(p.Elem, pe)
	}
	return p, nil
}

type errBadPath string

func (e errBadPath) Error() string { return "invalid gNMI path: " + string(e) }

func parseElem(s string) (*gpb.PathElem, error) {
	i := strings.IndexByte(s, '[')
	if i < 0 {
		if s == "" {
			return nil, errBadPath(s)
		}
		return &gpb.PathElem{Name: s}, nil
	}
	pe := &gpb.PathElem{Name: s[:i], Key: map[string]string{}}
	rest := s[i:]
	for len(rest) > 0 {
		if rest[0] != '[' {
			return nil, errBadPath(s)
		}
		// find unescaped ']'
		end := -1
		for j := 1; j < len(rest); j++ {
			if rest[j] == '\\' {
				j++
				continue
			}
			if rest[j] == ']' {
				end = j
				break
			}
		}
		if end < 0 {
			return nil, errBadPath(s)
		}
		kv := rest[1:end]
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			return nil, errBadPath(s)
		}
		v := strings.NewReplacer(`\]`, `]`, `\\`, `\`).Replace(kv[eq+1:])
		pe.Key[kv[:eq]] = v
		rest = rest[end+1:]
	}
	return pe, nil
}
