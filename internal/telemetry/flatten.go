package telemetry

import (
	"encoding/json"
	"strconv"
)

// FlattenJSON walks a decoded JSON value (as produced by encoding/json with
// UseNumber) and writes every scalar leaf into fields, keyed by its path
// relative to prefix, joined with "/". Arrays contribute their index as a
// path element. Numbers are stored as int64 when integral, uint64 when they
// overflow int64, and float64 otherwise.
func FlattenJSON(prefix string, v any, fields map[string]any) {
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			FlattenJSON(JoinPath(prefix, k), child, fields)
		}
	case []any:
		for i, child := range x {
			FlattenJSON(JoinPath(prefix, strconv.Itoa(i)), child, fields)
		}
	case json.Number:
		fields[leafName(prefix)] = ParseNumber(string(x))
	case string:
		fields[leafName(prefix)] = x
	case bool:
		fields[leafName(prefix)] = x
	case float64:
		fields[leafName(prefix)] = x
	case nil:
		// YANG "empty" leaves are encoded as [null]; record presence.
		fields[leafName(prefix)] = true
	}
}

func leafName(p string) string {
	if p == "" {
		return "value"
	}
	return p
}

// ParseNumber parses a JSON number into the narrowest canonical type.
func ParseNumber(s string) any {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if u, err := strconv.ParseUint(s, 10, 64); err == nil {
		return u
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

// JoinPath joins two path fragments with "/", ignoring empty parts.
func JoinPath(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	}
	return a + "/" + b
}
