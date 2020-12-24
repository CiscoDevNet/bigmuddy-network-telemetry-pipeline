//
// February 2016, cisco
//
// Copyright (c) 2016 by cisco Systdialin, Inc.
// All rights reserved.
//
//

package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	telem "github.com/cisco/bigmuddy-network-telemetry-proto/proto_go"
	dialin "github.com/cisco/bigmuddy-network-telemetry-proto/proto_go/mdt_grpc_dialin"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"math/rand"
	"net"
	"testing"
	"time"
)

type grpcTestUserPasswordCollector struct {
}

const (
	grpcServerPort = ":56789"
	GRPCTESTMSGS   = 10000
	GRPCCLIENTS    = 1
)

var dummyUsername = "root"
var dummyPassword = "lab"

func (c *grpcTestUserPasswordCollector) handleConfig(
	nc nodeConfig, name string, server string) error {
	return nil
}
func (c *grpcTestUserPasswordCollector) getUP() (
	string, string, error) {
	return dummyUsername, dummyPassword, nil
}

func grpcTestUPCollectorFactory() userPasswordCollector {
	return &grpcTestUserPasswordCollector{}
}

func TestGRPCRun(t *testing.T) {

	//
	// Setup username and password for test
	grpcUPCollector := grpcTestUPCollectorFactory()

	name := "xrv9k-grpc-server"
	server := grpcServerPort
	_, _, err := grpcUPCollector.getUP()
	if err != nil {
		t.Error("failed to collect username/password")
	}

	cleanSubs := extractSubscriptions(" counters, datarates, 132, nemo, sub1, sub2, sub3, sub4")
	ctrlChan := make(chan *ctrlMsg)
	dataChan := make(chan dataMsg, 10)

	var dataChans = make([]chan<- dataMsg, 0)
	dataChans = append(dataChans, dataChan)

	tls := false
	tls_pem := ""
	tls_key := ""
	tls_servername := ""

	block := &grpcBlock{
		name: name,
		server: &grpcRemoteServer{
			server:        server,
			subscriptions: cleanSubs,
			auth:          grpcUPCollector,
			reqID:         rand.Int63(),
		},
		encap:           ENCODING_GPB,
		encodingRequest: ENCODING_GPB_KV,
		logData:         true,
		ctrlChan:        ctrlChan,
		dataChans:       dataChans,
		childrenDone:    make(chan struct{}),
		tls:             tls,
		tls_pem:         tls_pem,
		tls_key:         tls_key,
		tls_servername:  tls_servername,
	}

	//
	// We will fail to connect. Test retry
	log.Debug("GRPC TEST RETRY: Transport errors expected here:")
	log.Debug("GRPC TEST RETRY: ==============================================")

	block.server.linkServerBlock(block)
	go block.run()
	time.Sleep(1 * time.Second)

	//
	// Now run local server and consume all
	go runTestGRPCServer(t)

	var i int
	for i = 0; i < GRPCTESTMSGS*GRPCCLIENTS*len(cleanSubs); i++ {
		<-dataChan
	}
	log.Debug("       END       ==============================================")

	log.Debug("GRPCTEST: exited datachannel after %d events\n", i)
	respChan := make(chan *ctrlMsg)
	request := &ctrlMsg{
		id:       SHUTDOWN,
		respChan: respChan,
	}

	//
	// and shutdown in the face of retry.
	ctrlChan <- request

	// Wait for ACK
	ack := <-respChan

	if ack.id != ACK {
		t.Error("failed to recieve acknowledgement indicating shutdown complete")
	}

	//
	// Now run local server and consume half then cancel
	ctrlChan2 := make(chan *ctrlMsg)
	dataChan2 := make(chan dataMsg, 10)
	var dataChans2 = make([]chan<- dataMsg, 0)
	dataChans2 = append(dataChans2, dataChan2)
	block2 := &grpcBlock{
		name: "client2TestForCancel",
		server: &grpcRemoteServer{
			server:        server,
			subscriptions: cleanSubs,
			auth:          grpcUPCollector,
			reqID:         rand.Int63(),
		},
		encap:           ENCODING_GPB,
		encodingRequest: ENCODING_GPB_KV,
		logData:         true,
		ctrlChan:        ctrlChan2,
		dataChans:       dataChans2,
		childrenDone:    make(chan struct{}),
		tls:             tls,
		tls_pem:         tls_pem,
		tls_servername:  tls_servername,
	}
	block2.server.linkServerBlock(block2)
	go block2.run()

	for i = 0; i < (GRPCTESTMSGS/2)*GRPCCLIENTS*len(cleanSubs); i++ {
		<-dataChan2
	}

	log.Debug("GRPCTEST: exited datachannel after %d events"+
		" (partial, as expect), test cancel\n", i)

	respChan = make(chan *ctrlMsg)
	request = &ctrlMsg{
		id:       SHUTDOWN,
		respChan: respChan,
	}

	//
	// and shutdown in the face of retry.
	ctrlChan2 <- request

	// Wait for ACK
	ack = <-respChan

	if ack.id != ACK {
		t.Error("failed to recieve acknowledgement indicating shutdown complete")
	}

}

type gRPCConfigOperServer struct{}

func (s *gRPCConfigOperServer) GetConfig(*dialin.ConfigGetArgs,
	dialin.GRPCConfigOper_GetConfigServer) error {
	return nil
}
func (s *gRPCConfigOperServer) MergeConfig(context.Context, *dialin.ConfigArgs) (*dialin.ConfigReply, error) {
	return nil, nil
}
func (s *gRPCConfigOperServer) DeleteConfig(context.Context, *dialin.ConfigArgs) (*dialin.ConfigReply, error) {
	return nil, nil
}
func (s *gRPCConfigOperServer) ReplaceConfig(context.Context, *dialin.ConfigArgs) (*dialin.ConfigReply, error) {
	return nil, nil
}
func (s *gRPCConfigOperServer) CliConfig(context.Context, *dialin.CliConfigArgs) (*dialin.CliConfigReply, error) {
	return nil, nil

}
func (s *gRPCConfigOperServer) CommitReplace(context.Context, *dialin.CommitReplaceArgs) (*dialin.CommitReplaceReply, error) {
	return nil, nil
}
func (s *gRPCConfigOperServer) CommitConfig(context.Context, *dialin.CommitArgs) (*dialin.CommitReply, error) {
	return nil, nil
}
func (s *gRPCConfigOperServer) ConfigDiscardChanges(context.Context, *dialin.DiscardChangesArgs) (*dialin.DiscardChangesReply, error) {
	return nil, nil
}
func (s *gRPCConfigOperServer) GetOper(*dialin.GetOperArgs, dialin.GRPCConfigOper_GetOperServer) error {
	return nil
}

//
// Create subs handler.
func (s *gRPCConfigOperServer) CreateSubs(
	reqArgs *dialin.CreateSubsArgs,
	stream dialin.GRPCConfigOper_CreateSubsServer) error {

	ctx := stream.Context()
	md, _ := metadata.FromContext(ctx)

	if md["username"][0] != dummyUsername {
		return fmt.Errorf("Bad username")
	}

	if md["password"][0] != dummyPassword {
		return fmt.Errorf("Bad password")
	}

	var collectionId uint64

	collectionId = 100
	basePath := "RootOper.ABC.DEF"

	var msg telem.Telemetry

	if reqArgs.Encode == GRPC_ENCODE_GPBKV {
		msg = telem.Telemetry{
			CollectionId:        collectionId,
			EncodingPath:        basePath,
			CollectionStartTime: uint64(time.Now().Unix()),
			CollectionEndTime:   uint64(time.Now().Unix()),
		}
	} else {
		return fmt.Errorf("Requesting unexpected encoding")
	}

	msgstream, _ := proto.Marshal(&msg)

	for i := 0; i < GRPCTESTMSGS; i++ {

		reply := dialin.CreateSubsReply{
			ResReqId: reqArgs.ReqId,
			Data:     msgstream,
			Errors:   "",
		}

		if err := stream.Send(&reply); err != nil {
			return err
		}

	}

	return nil
}

func runTestGRPCServer(t *testing.T) {

	lis, err := net.Listen("tcp", grpcServerPort)
	if err != nil {
		t.Logf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	dialin.RegisterGRPCConfigOperServer(s, new(gRPCConfigOperServer))
	go s.Serve(lis)

}
