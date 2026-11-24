package gnmi

import (
	"testing"

	gpb "github.com/openconfig/gnmi/proto/gnmi"
	"google.golang.org/protobuf/proto"
)

func mustPath(t *testing.T, s string) *gpb.Path {
	t.Helper()
	p, err := ParsePath("", s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestParsePath(t *testing.T) {
	p, err := ParsePath("", `openconfig:/interfaces/interface[name=Ethernet1/1]/subinterfaces/subinterface[index=0]/state`)
	if err != nil {
		t.Fatal(err)
	}
	want := &gpb.Path{Origin: "openconfig", Elem: []*gpb.PathElem{
		{Name: "interfaces"},
		{Name: "interface", Key: map[string]string{"name": "Ethernet1/1"}},
		{Name: "subinterfaces"},
		{Name: "subinterface", Key: map[string]string{"index": "0"}},
		{Name: "state"},
	}}
	if !proto.Equal(p, want) {
		t.Fatalf("got %v\nwant %v", p, want)
	}
	p, err = ParsePath("", `/a[k=x\]y][j=2]/b`)
	if err != nil || p.Elem[0].Key["k"] != "x]y" || p.Elem[0].Key["j"] != "2" {
		t.Fatalf("escaped keys: %v %v", p, err)
	}
	for _, bad := range []string{"/a[k=1", "/a[=1]", "/a//b"} {
		if _, err := ParsePath("", bad); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
	if p, _ := ParsePath("", "/"); len(p.Elem) != 0 {
		t.Error("root path should have no elems")
	}
}

func TestConvertGroupsByInstance(t *testing.T) {
	n := &gpb.Notification{
		Timestamp: 1_700_000_000_000_000_000,
		Prefix:    &gpb.Path{Origin: "openconfig", Elem: []*gpb.PathElem{{Name: "interfaces"}}},
		Update: []*gpb.Update{
			{Path: mustPath(t, "/interface[name=Eth1]/state/counters/in-octets"), Val: &gpb.TypedValue{Value: &gpb.TypedValue_UintVal{UintVal: 100}}},
			{Path: mustPath(t, "/interface[name=Eth1]/state/oper-status"), Val: &gpb.TypedValue{Value: &gpb.TypedValue_StringVal{StringVal: "UP"}}},
			{Path: mustPath(t, "/interface[name=Eth2]/state/counters/in-octets"), Val: &gpb.TypedValue{Value: &gpb.TypedValue_UintVal{UintVal: 7}}},
			{Path: mustPath(t, "/interface[name=Eth1]/subinterfaces/subinterface[index=0]/state/counters/in-pkts"), Val: &gpb.TypedValue{Value: &gpb.TypedValue_IntVal{IntVal: 3}}},
			{Path: mustPath(t, "/interface[name=Eth1]/state/x"), Val: &gpb.TypedValue{Value: &gpb.TypedValue_AnyVal{}}},
		},
	}
	ms, unsupported := Convert(n, map[string]string{"source": "r1"})
	if unsupported != 1 {
		t.Errorf("unsupported=%d", unsupported)
	}
	if len(ms) != 3 {
		t.Fatalf("expected 3 metrics, got %d: %+v", len(ms), ms)
	}
	eth1 := ms[0]
	if eth1.Name != "openconfig:/interfaces/interface" || eth1.Tags["interface_name"] != "Eth1" || eth1.Tags["source"] != "r1" {
		t.Errorf("eth1 identity wrong: %+v", eth1)
	}
	if eth1.Fields["state/counters/in-octets"] != uint64(100) || eth1.Fields["state/oper-status"] != "UP" {
		t.Errorf("eth1 fields wrong: %+v", eth1.Fields)
	}
	if eth1.Timestamp.UnixNano() != 1_700_000_000_000_000_000 {
		t.Errorf("timestamp wrong")
	}
	sub := ms[2]
	if sub.Name != "openconfig:/interfaces/interface/subinterfaces/subinterface" ||
		sub.Tags["interface_name"] != "Eth1" || sub.Tags["subinterface_index"] != "0" ||
		sub.Fields["state/counters/in-pkts"] != int64(3) {
		t.Errorf("subinterface metric wrong: %+v", sub)
	}
}

func TestConvertJSONIETF(t *testing.T) {
	n := &gpb.Notification{
		Update: []*gpb.Update{{
			Path: mustPath(t, "/openconfig-interfaces:interfaces/interface[name=Eth1]/state"),
			Val: &gpb.TypedValue{Value: &gpb.TypedValue_JsonIetfVal{JsonIetfVal: []byte(
				`{"openconfig-interfaces:counters":{"in-octets":"18446744073709551615","in-errors":"0"},"mtu":9000,"enabled":true,"openconfig-if-ethernet:speed":"SPEED_100GB"}`)}},
		}},
	}
	ms, _ := Convert(n, nil)
	if len(ms) != 1 {
		t.Fatalf("got %d metrics", len(ms))
	}
	f := ms[0].Fields
	want := map[string]any{
		"state/counters/in-octets": uint64(18446744073709551615),
		"state/counters/in-errors": int64(0),
		"state/mtu":                int64(9000),
		"state/enabled":            true,
		"state/speed":              "SPEED_100GB",
	}
	for k, v := range want {
		if f[k] != v {
			t.Errorf("%s = %#v, want %#v (all: %v)", k, f[k], v, f)
		}
	}
	if ms[0].Name != "/interfaces/interface" {
		t.Errorf("module prefix not stripped from name: %s", ms[0].Name)
	}
}

func TestConvertMisc(t *testing.T) {
	n := &gpb.Notification{Update: []*gpb.Update{
		{Path: mustPath(t, "/system/state/hostname"), Val: &gpb.TypedValue{Value: &gpb.TypedValue_StringVal{StringVal: "r1"}}},
		{Path: mustPath(t, "/system/cpu/load"), Val: &gpb.TypedValue{Value: &gpb.TypedValue_DecimalVal{DecimalVal: &gpb.Decimal64{Digits: 1234, Precision: 2}}}},
		{Path: mustPath(t, "/system/dns/servers"), Val: &gpb.TypedValue{Value: &gpb.TypedValue_LeaflistVal{LeaflistVal: &gpb.ScalarArray{
			Element: []*gpb.TypedValue{{Value: &gpb.TypedValue_StringVal{StringVal: "1.1.1.1"}}, {Value: &gpb.TypedValue_StringVal{StringVal: "8.8.8.8"}}}}}}},
	}}
	ms, _ := Convert(n, nil)
	if len(ms) != 3 {
		t.Fatalf("got %d", len(ms))
	}
	if ms[0].Name != "/system/state" || ms[0].Fields["hostname"] != "r1" {
		t.Errorf("keyless path wrong: %+v", ms[0])
	}
	if ms[1].Fields["load"] != 12.34 {
		t.Errorf("decimal wrong: %v", ms[1].Fields)
	}
	if ms[2].Fields["servers"] != "1.1.1.1,8.8.8.8" {
		t.Errorf("leaflist wrong: %v", ms[2].Fields)
	}
}
