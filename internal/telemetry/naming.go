package telemetry

import "strings"

// SanitizeName converts an arbitrary path such as
// "openconfig:/interfaces/interface/state/counters/in-octets" into a
// Prometheus/OpenMetrics-safe identifier:
// "interfaces_interface_state_counters_in_octets".
//
// The origin prefix ("openconfig:") is dropped because it is not part of the
// data schema; YANG module prefixes on individual elements are dropped too
// ("openconfig-interfaces:interfaces" -> "interfaces"). Cisco native paths
// keep their module name since it is the first path element.
func SanitizeName(path string) string {
	var b strings.Builder
	b.Grow(len(path))
	lastUnderscore := true // suppress leading underscore
	for _, elem := range splitPath(path) {
		for _, r := range elem {
			ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
			if ok {
				b.WriteRune(r)
				lastUnderscore = false
			} else if !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	s := strings.TrimRight(b.String(), "_")
	if s == "" {
		return "value"
	}
	if s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}
	return s
}

// SanitizeLabel converts a tag key into a valid Prometheus label name.
func SanitizeLabel(k string) string {
	s := SanitizeName(k)
	if strings.HasPrefix(s, "__") {
		s = "x" + s // reserved prefix
	}
	return s
}

// splitPath splits on "/" and strips origin / module prefixes on each element.
func splitPath(path string) []string {
	// Drop a gNMI origin prefix of the form "origin:/".
	if i := strings.Index(path, ":/"); i >= 0 && !strings.Contains(path[:i], "/") {
		path = path[i+2:]
	}
	parts := strings.Split(path, "/")
	out := parts[:0]
	for i, p := range parts {
		if p == "" {
			continue
		}
		// Strip "module:" prefixes, except on the very first element of a
		// native vendor path ("Cisco-IOS-XR-foo-oper:bar") where the module
		// name carries meaning.
		if j := strings.IndexByte(p, ':'); j >= 0 {
			if i == 0 && len(out) == 0 && !strings.HasPrefix(path, "/") {
				p = p[:j] + "_" + p[j+1:]
			} else {
				p = p[j+1:]
			}
		}
		out = append(out, p)
	}
	return out
}
