## Streaming Telemetry

 Go Proto Library for Cisco IOS-XR Model Driven Telemetry

________________________________________________________________________________________________


The content in `proto_go` directory is automatically generated and
only committed to the repo for convenience.

Should it be necessary to regenerate a golang binding, simply run
`prep_golang.py` from the telemetry_proto root. This script will build
the golang proto bindings based on content in the `staging` directory,
generate code and tests, and run the tests, including performance
benchmarks.

In some more detail, `prep_golang.py` takes care of setting up
`docs.go` with `go generate` directives to generate golang bindings
from `.proto` files, running `go generate` to generate the golang
bindings for the .proto files, as well as generating the wrapper code
to serve objects to unmarshal into against the encoding path. Once the
wrapper code is generated, it is built, and auto generated tests are
run.

Package documentation for golang can be browsed by firing up "godoc
-http=:6060 &" picking which every port is covenient, and then point
browser to "http://<host>:6060/pkg/telemetry_proto/proto_go/".

A sample run looks like this:

```

[gows/src/telemetry_proto]$./prep_golang.py
Generating proto_go/mdt_telemetry.go...
Generating proto_go/mdt_telemetry_test.go...
Copying proto_go/doc.go...
Soft links to protos and adding 'go generate' directive in doc.go...
Generating golang bindings for 668 .proto files. This stage takes some time...
Building and running tests for package. The build stage also takes some time...

...
    --- PASS: TestYang2Proto/Cisco-IOS-XR-mpls-ldp-oper:mpls-ldp/global/active/default-vrf/afs/af/discovery/summary (0.00s)
    --- PASS: TestYang2Proto/Cisco-IOS-XR-ipv4-bgp-oper:bgp/instances/instance/instance-active/default-vrf/bmp/server-summaries/server-summary (0.00s)
    --- PASS: TestYang2Proto/Cisco-IOS-XR-mpls-ldp-oper:mpls-ldp/nodes/node/vrfs/vrf/afs/af/forwarding-summary (0.00s)
    --- PASS: TestYang2Proto/Cisco-IOS-XR-infra-tc-oper:traffic-collector/summary (0.00s)
    --- PASS: TestYang2Proto/Cisco-IOS-XR-ipv4-bgp-oper:bgp/instances/instance/instance-active/default-vrf/afs/af/advertised-paths/advertised-path (0.00s)
    --- PASS: TestYang2Proto/Cisco-IOS-XR-ipv4-bgp-oper:bgp/instances/instance/instance-standby/vrfs/vrf/update-inbound-error-vrf (0.00s)
=== RUN   ExampleEncodingPathToMessageReflectionSet
--- PASS: ExampleEncodingPathToMessageReflectionSet (0.00s)
=== RUN   ExampleProtoMsgs_MessageReflection
--- PASS: ExampleProtoMsgs_MessageReflection (0.00s)
BenchmarkMessageSetLookup-80            20000000               101 ns/op
PASS
ok      telemetry_proto/proto_go        8.139s
Done.
```


### Dependencies

In order to build the golang bigmuddy-network-telemetry-proto package,
[go1.8](https://golang.org/doc/install) and
[protoc](https://github.com/google/protobuf/releases) are required.

Note that given we are building a package, we do not vendor
dependencies. The only dependency is protobuf.

```
go get -u github.com/golang/protobuf/{proto,protoc-gen-go}
./prep_golang.py
```