# Adding proto files to Pipeline

As described in [Go Proto Library for Cisco IOS-XR Model Driven Telemetry](https://github.com/cisco/bigmuddy-network-telemetry-proto/tree/master/proto_go), the content in [proto_go](vendor/github.com/cisco/bigmuddy-network-telemetry-proto/proto_go/) folder is automatically generated and only committed to the repo for convenience.

In order to generate a golang binding, simply run [prep_golang.py](vendor/github.com/cisco/bigmuddy-network-telemetry-proto/prep_golang.py) from [telemetry_proto](vendor/github.com/cisco/bigmuddy-network-telemetry-proto/) root. This script will build the golang proto bindings based on content in the [staging](vendor/github.com/cisco/bigmuddy-network-telemetry-proto/proto_go/) folder using protoc compiler. Then you can re-compile Pipeline.

## Table of contents
  * [Requirements](#requirements)
    + [Go](#go)
    + [Protoc](#protoc)
    + [Go protoc plugin](#go-protoc-plugin)
    + [XR Proto archive](#xr-proto-archive)
  * [The actual procedure](#the-actual-procedure)
    + [Clone Pipeline repo](#clone-pipeline-repo)
    + [Add proto files to staging](#add-proto-files-to-staging)
      - [Are proto files are in the archive?](#are-proto-files-are-in-the-archive-)
      - [If they are not](#if-they-are-not)
      - [Copy proto files to the staging](#copy-proto-files-to-the-staging)
    + [Key mappings](#key-mappings)
    + [Generate Go code](#generate-go-code)
    + [Re-compile Pipeline](#re-compile-pipeline)

## Requirements

### Go 

Follow the instruction on [Getting Started](https://golang.org/doc/install). This is just an example:

``` bash
wget https://storage.googleapis.com/golang/go1.8.1.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.8.1.linux-amd64.tar.gz
```

Create a workspace and set the Go environment variables.

``` bash
mkdir ~/go
export GOPATH=$HOME/go
export GOROOT=/usr/local/go
export PATH=$PATH:$GOPATH/bin:$GOROOT/bin
```

You can verify Go is installed with `go version`

``` bash    
$ go version
go version go1.8.1 linux/amd64    
```
    
### Protoc
 
Install the Protocol Buffers compiler as depicted in here: [Protocol Buffers](https://github.com/google/protobuf/tree/master/src). This is just an example:

``` bash    
sudo apt-get install autoconf automake libtool curl make g++ unzip
git clone https://github.com/google/protobuf.git
 cd protobuf
./autogen.sh
./configure --prefix=/usr
make
make check
sudo make install
sudo ldconfig
```

You can verify the protobuf compiler is installed with `protoc --version`

``` bash   
$ protoc --version
libprotoc 3.3.0
```

### Go protoc plugin

Install the protoc plugin for Go, check out [Go support for Protocol Buffers](https://github.com/golang/protobuf). This is just an example:

``` bash  
go get -u github.com/golang/protobuf/{proto,protoc-gen-go}
```

### XR Proto archive

While dependencies in Pipeline are vendored in using [glide](glide.yaml), you need the [Streaming telemetry protobuf archive](https://github.com/cisco/bigmuddy-network-telemetry-proto) in your workspace to generate the Go bindings.

``` bash 
go get github.com/cisco/bigmuddy-network-telemetry-proto
```

## The actual procedure


### Clone Pipeline repo

You can clone the master version if you are not planning to upstream your changes:

``` bash 
go get github.com/cisco/bigmuddy-network-telemetry-pipeline
```

Or fork the code and work on that branch:

``` bash 
go get github.com/nleiva/bigmuddy-network-telemetry-pipeline
```

### Add proto files to staging

As an example, we will add `Cisco-IOS-XR-controller-optics-oper` proto files to the staging folder

#### Are proto files are in the archive?

Check if the proto files are already in the archive. Keep in mind dash ("-") is replaced by underscore ("_")

``` bash 
$ cd $GOPATH/src/github.com/cisco/bigmuddy-network-telemetry-pipeline/
$ ls vendor/github.com/cisco/bigmuddy-network-telemetry-proto/proto_archive/ | grep optics
cisco_ios_xr_controller_optics_oper
$
```

#### If they are not

You will need to download the proto files from the device and copy over.

#### Copy proto files to the staging 

Copy the proto files to staging folder

``` bash 
cd $GOPATH/src/github.com/cisco/bigmuddy-network-telemetry-pipeline/vendor/github.com/cisco/bigmuddy-network-telemetry-proto/proto_archive/
cp -r cisco_ios_xr_controller_optics_oper ../staging
```

### Key mappings 

Files ending in yang2proto_map.json found in staging contain the metadata required to map the keys received, from streaming devices, to the service that unmarshals the content of the message. Check out [Support for unmarshalling telemetry messages](/vendor/github.com/cisco/bigmuddy-network-telemetry-proto#support-for-unmarshaling-telemetry-messages). 

There is one file already present; [cisco_ios_xr_yang2proto_map.json](vendor/github.com/cisco/bigmuddy-network-telemetry-proto/staging/cisco_ios_xr_yang2proto_map.json). Verify if it already has the key for the path of interest

``` bash 
$ cd $GOPATH/src/github.com/cisco/bigmuddy-network-telemetry-pipeline/vendor/github.com/cisco/bigmuddy-network-telemetry-proto/staging
$ grep cisco_ios_xr_controller_optics_oper cisco_ios_xr_yang2proto_map.json
{"file": "cisco_ios_xr_controller_optics_oper/optics_oper/optics_ports/optics_port/optics_info/optics_edm_info.proto",
"package": "cisco_ios_xr_controller_optics_oper.optics_oper.optics_ports.optics_port.optics_info",
{"file": "cisco_ios_xr_controller_optics_oper/optics_oper/optics_ports/optics_port/optics_dwdm_carrrier_channel_map/optics_edm_wave_info.proto",
"package": "cisco_ios_xr_controller_optics_oper.optics_oper.optics_ports.optics_port.optics_dwdm_carrrier_channel_map",
$
```

### Generate Go code

Generate the Go source code for these proto files with protoc. There is actually a script that will take care of this among other things; [prep_golang.py](vendor/github.com/cisco/bigmuddy-network-telemetry-proto/prep_golang.py).

``` bash 
$ cd $GOPATH/src/github.com/cisco/bigmuddy-network-telemetry-pipeline/vendor/github.com/cisco/bigmuddy-network-telemetry-proto/
$ ./prep_golang.py
Generating proto_go/mdt_telemetry.go...
Generating proto_go/mdt_telemetry_test.go...
Copying proto_go/doc.go...
Copying proto_go/README.md...
Generating proto_go/basepathxlation.json...
Soft links to protos and adding 'go generate' directive in a.go...
Generating golang bindings for 585 .proto files. This stage takes some time...
Building and running tests for package. The build stage also takes some time...
BenchmarkMessageSetLookup-32            20000000                97.2 ns/op
PASS
ok      github.com/cisco/bigmuddy-network-telemetry-proto/proto_go      4.794s
Done.
$
```

### Re-compile Pipeline

And finally...

``` bash 
cd $GOPATH/src/github.com/cisco/bigmuddy-network-telemetry-pipeline
go build -o bin/pipeline 
```
