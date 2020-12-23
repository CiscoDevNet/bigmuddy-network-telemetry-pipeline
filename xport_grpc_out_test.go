package main

import (
	_ "fmt"
	log "github.com/sirupsen/logrus"
	samples "github.com/cisco/bigmuddy-network-telemetry-pipeline/mdt_msg_samples"
	"github.com/dlintw/goconf"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type grpcOutTestClient struct {
	t *testing.T
}

func (c *grpcOutTestClient) GetRequestMetadata(
	ctx context.Context, uri ...string) (
	map[string]string, error) {

	mapping := map[string]string{}

	return mapping, nil
}

func (c *grpcOutTestClient) RequireTransportSecurity() bool {
	return false
}

func (c *grpcOutTestClient) grpcOutTestClientRun(
	server string,
	timeout int,
	target int,
	result chan bool) {

	var opts []grpc.DialOption
	var ctx context.Context

	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithPerRPCCredentials(c))
	opts = append(opts,
		grpc.WithTimeout(time.Second*time.Duration(timeout)))

	conn, err := grpc.Dial(server, opts...)
	if err != nil {
		c.t.Log("client dial failed", server, err, timeout)
		result <- false
		return
	}
	defer conn.Close()

	req := &SubJSONReqMsg{
		ReqId: rand.Int63(),
	}

	ctx, _ = context.WithCancel(context.Background())
	client := NewGRPCOutClient(conn)
	stream, err := client.Pull(ctx, req)
	if err != nil {
		c.t.Log("client pull failed", server, err)
		result <- false
		return
	}

	i := 0
	for {
		reply, err := stream.Recv()
		if err == nil {
			i++
			if req.ReqId != reply.ReqId {
				c.t.Log("reqId mismatch", server, req.ReqId, reply.ReqId)
			}
			if i == target {
				result <- true
				stream.CloseSend()
				if _, err := stream.Recv(); err != nil {
					break
				}
				return
			}
		} else if err == io.EOF {
			// c.t.Log("client recv EOF", server, err)
			break
		} else {
			// c.t.Error("client recv failed", server, err)
			result <- false
			return
		}
	}
}

type grpcOutProducer struct {
	t     *testing.T
	dMch  chan<- dataMsg
	codec codec
}

func (g *grpcOutProducer) String() string {
	return "grpc_out_test"
}

func grpcOutProduceOneIteration(
	sample *samples.SampleTelemetryTableEntry,
	context samples.MDTContext) (abort bool) {

	c := context.(*grpcOutProducer)

	err, msgs := c.codec.blockToDataMsgs(c, sample.SampleStreamGPB)
	if err != nil {
		c.t.Fatal("producing one iteration blockToDataMsgs failed", err)
	}

	for _, msg := range msgs {
		// fmt.Print("Dispatching msg\n")
		c.dMch <- msg
	}

	return false
}

func grpcOutProduceContent(
	t *testing.T,
	ctrl chan struct{},
	dMch chan<- dataMsg,
	iterations int,
	periodSeconds int) {

	err, codec := getNewCodecGPB("test", ENCODING_GPB)
	if err != nil {
		t.Fatal("Failed to get codec", err)
	}

	prod := &grpcOutProducer{
		t:     t,
		dMch:  dMch,
		codec: codec,
	}

	for {
		// fmt.Print("Iteration\n")
		for i := 0; i < iterations; i++ {
			samples.MDTSampleTelemetryTableIterate(
				samples.SAMPLE_TELEMETRY_DATABASE_BASIC,
				grpcOutProduceOneIteration, prod)
			select {
			case <-ctrl:
				return
			default:
			}
		}
		timeout := make(chan bool, 1)
		go func() {
			// fmt.Printf("Wait %vs \n", periodSeconds)
			time.Sleep(time.Duration(periodSeconds) * time.Second)
			timeout <- true
		}()
		select {
		case <-ctrl:
			// We're done here
			return
		case <-timeout:
			// Wait for timeout specified
			// fmt.Print("Period\n")
		}
	}
}

func TestGrpcOutConfigNegative(t *testing.T) {

	var nc nodeConfig
	var err error

	startup()
	logger = theLogger.WithFields(log.Fields{"tag": "test"})

	//
	// Read test config
	nc.config, err = goconf.ReadConfigFile("pipeline_test.conf")
	if err != nil {
		t.Error("Config failed to open config pipeline_test.conf")
	}
	g := grpcOutputModuleNew()

	err, _, _ = g.configure("mygrpcoutnolisten", nc)
	if err == nil {
		t.Error("Config expected to fail without 'listen'")
	}
	err, _, _ = g.configure("mygrpcoutbadencoding", nc)
	if err == nil {
		t.Error("Config expected to fail unsupported 'encoding'")
	}
}

func coreTestGrpcOutClient(
	t *testing.T,
	clients int,
	collect int,
	produce_iterations int,
	period int,
	earlyTerminationAfter int,
	produceOnly bool) {

	var nc nodeConfig
	var err error

	startup()

	//
	// Read test config
	nc.config, err = goconf.ReadConfigFile("pipeline_test.conf")
	if err != nil {
		log.Fatalf("Pipeline_test.conf could not be opened [%v]", err)
	}
	go metamonitoring_init(nc)

	o := grpcOutputModuleNew()

	err, dM, ctrl := o.configure("mygrpcout", nc)
	if err != nil {
		t.Fatal("Failed to configure module", err)
	}
	// Wait for configure to complete before we start test
	time.Sleep(time.Second)
	//
	// Kick off client
	c := &grpcOutTestClient{t: t}
	server, err := nc.config.GetString("mygrpcout", "listen")
	if err != nil {
		t.Fatal("Failed to pick up 'listen'", err)
	}

	produceCtrl := make(chan struct{})
	results := make(chan bool, clients)
	if !produceOnly {
		for i := 0; i < clients; i++ {
			//
			// Given this uses t we should synchronise and make sure
			// it does not outlast the test.
			go c.grpcOutTestClientRun(server, 3, collect, results)
		}
	}

	go grpcOutProduceContent(t, produceCtrl, dM, produce_iterations, period)

	if produceOnly {
		//
		// block forever
		var wg sync.WaitGroup
		wg.Add(1)
		wg.Wait()
	}

	success := 0
	if earlyTerminationAfter == 0 {
		for result := range results {
			if result {
				success++
				if success == clients {
					break
				}
			} else {
				t.Fatal("Client returned failure")
			}
		}
	} else {
		time.Sleep(time.Second * time.Duration(period*earlyTerminationAfter))
	}

	//
	// Stop producing
	close(produceCtrl)

	//
	// Close module from top
	respChan := make(chan *ctrlMsg)
	request := &ctrlMsg{
		id:       SHUTDOWN,
		respChan: respChan,
	}

	ctrl <- request
	// Wait for ACK
	<-respChan
	close(ctrl)
}

func TestGrpcOutClient(t *testing.T) {
	clients := 10
	// collect so many messages before returning ok at client
	collect := 14
	// number of interations to produce - 2xmsgs per iteration
	produce_iterations := 5 // 5x2 = 10, i.e. at least two iterations
	// period between iterations
	period := 2
	coreTestGrpcOutClient(t, clients, collect, produce_iterations, period, 0, false)
}

func TestGrpcOutClientCloseServerSide(t *testing.T) {
	// Wait for port to be available again
	time.Sleep(time.Second * 3)
	clients := 10
	// collect so many messages before returning ok at client
	collect := 9999999
	// number of iterations to produce - 2xmsgs per iteration
	produce_iterations := 5 // 5x2 = 10, i.e. 999999/10 - very many iterations
	// period between iterations
	period := 1
	coreTestGrpcOutClient(t, clients, collect, produce_iterations, period, 3, false)
}
