//
// February 2016, cisco
//
// Copyright (c) 2016 by cisco Systdialin. Inc.
// All rights reserved.
//
//
// gRPC client for server side streaming telemetry, and server for
// client side streaming.
//
// The client sets up a connection with the server, subscribes for all
// paths, and then kicks off go routines to support each subscription,
// whilst watching for cancellation from the conductor
//
// The server listens for gRPC connections from routers and then
// collects telemetry streams from gRPC clients.
//
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	dialin "github.com/cisco/bigmuddy-network-telemetry-proto/proto_go/mdt_grpc_dialin"
	dialout "github.com/cisco/bigmuddy-network-telemetry-proto/proto_go/mdt_grpc_dialout"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	XPORT_GRPC_WAIT_TO_REDIAL = 1
	GRPC_TIMEOUT              = 10
	//
	// equivalent of mdtbk_encoding_t on server side
	GRPC_ENCODE_GPB   = 2
	GRPC_ENCODE_GPBKV = 3
	GRPC_ENCODE_JSON  = 4
)

//
// Common prefix for all log message for consistency
const GRPCLOGPRE = "gRPC: "

func mapEncapToGRPCEncode(encap encoding) (error, int64) {

	mapping := map[encoding]int64{
		ENCODING_GPB_COMPACT: GRPC_ENCODE_GPB,
		ENCODING_GPB_KV:      GRPC_ENCODE_GPBKV,
		ENCODING_JSON:        GRPC_ENCODE_JSON,
	}

	grpcEnc, ok := mapping[encap]
	if !ok {
		return fmt.Errorf(GRPCLOGPRE+
			"Failed to map requested encap %v to gRPC",
			encap), 0
	}

	return nil, grpcEnc
}

//
// Track client configuration
type grpcBlock struct {
	name string
	//
	// Server will point at a local or remote server.
	server          grpcServer
	encap           encoding
	encodingRequest encoding
	// Log data into debug log
	logData bool
	// Credentials
	tls            bool
	tls_pem        string
	tls_key        string
	tls_servername string
	// Control channel used to control the gRPC client
	ctrlChan <-chan *ctrlMsg
	// Data channels fed by the server
	dataChans []chan<- dataMsg
	// Use to signal that all children have shutdown.
	childrenDone chan struct{}
	// Setup a consistent log entry to build logging from
	logctx *log.Entry
}

type grpcServer interface {
	//
	// Do the business. Run the sticky loop which tries hard to keep
	// receiving streams UNTIL the context is closed.
	stickyLoop(ctx context.Context)
	//
	// The local or remote socket for the server end
	String() string
	//
	// Extend the logger context with server type specific context
	ExtendLogContext(*log.Entry) *log.Entry
	//
	// Track the common server block
	linkServerBlock(c *grpcBlock)
}

///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////
///////              D I A L I N    H A N D L E R               ///////
///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////

type grpcRemoteServer struct {
	server        string
	subscriptions []string
	reqID         int64
	auth          userPasswordCollector
	common        *grpcBlock
}

func (s *grpcRemoteServer) String() string {
	return s.server
}

func (s *grpcRemoteServer) ExtendLogContext(logctx *log.Entry) *log.Entry {
	username, _, _ := s.auth.getUP()
	return logctx.WithFields(log.Fields{
		"type":            "pipeline is CLIENT",
		"subscriptions":   s.subscriptions,
		"username":        username,
		"encap":           encodingToName(s.common.encap),
		"encodingRequest": encodingToName(s.common.encodingRequest),
	})
}

func (s *grpcRemoteServer) linkServerBlock(c *grpcBlock) {
	s.common = c
}

//
//  Handle a single subscription. This function is run as an
//  independent go routine and its role is to receive messages in a
//  stream, and dispatch the decoded content onto the downstream
//  channels. Any failures will cause the stream handling to terminate
//  and indicate as much to the wait group. Equally, if the
//  subscription is cancelled we bail out.
func singleSubscription(
	thisCtx context.Context,
	thisServer *grpcRemoteServer,
	thisSub string,
	thisReqID int64,
	thisStream dialin.GRPCConfigOper_CreateSubsClient,
	thisCodec codec,
	thisDataChans []chan<- dataMsg,
	thisWg *sync.WaitGroup,
	logctx *log.Entry,
	logData bool) {

	defer thisWg.Done()

	logctx.WithFields(
		log.Fields{
			"subscription": thisSub,
			"reqID":        thisReqID,
		}).Info(GRPCLOGPRE + "Subscription handler running")

	for {
		reply, err := thisStream.Recv()
		select {
		case <-thisCtx.Done():
			count := 0
			thisStream.CloseSend()
			for {
				if _, err := thisStream.Recv(); err != nil {
					logctx.WithFields(
						log.Fields{
							"subscription": thisSub,
							"reqID":        thisReqID,
							"count":        count,
						}).Info(GRPCLOGPRE +
						"Drained on cancellation")
					break
				}
				count++
			}
			return
		default:
			if err != nil {
				logctx.WithError(err).WithFields(
					log.Fields{
						"subscription": thisSub,
						"reqID":        thisReqID,
					}).Error(GRPCLOGPRE +
					"Server terminated sub")
				return
			}
			if reply.Errors != "" {
				logctx.WithError(err).WithFields(
					log.Fields{
						"subscription": thisSub,
						"reqID":        thisReqID,
						"report":       reply.Errors,
					}).Error(GRPCLOGPRE +
					"Server terminate sub")
				return
			}

			if logData {
				logctx.WithFields(log.Fields{
					"msg": hex.Dump(reply.Data),
				}).Debug("gRPC server logdata")

			}

			err, dMs := thisCodec.blockToDataMsgs(
				thisServer, reply.Data)
			if err != nil {
				logctx.WithError(err).WithFields(
					log.Fields{
						"subscription": thisSub,
						"reqID":        thisReqID,
					}).Error(GRPCLOGPRE +
					"Extracting msg from stream failed")
				return
			}

			if len(dMs) == 0 {
				logctx.WithFields(
					log.Fields{
						"subscription": thisSub,
						"reqID":        thisReqID,
					}).Error(GRPCLOGPRE +
					"Extracting msg from stream came up empty")
			}

			if dMs != nil {
				for _, dM := range dMs {
					//
					// Push data onto channel.
					for _, dataChan := range thisDataChans {
						//
						// Make sure that if
						// we are blocked on
						// consumer, we still
						// handle cancel.
						select {
						case <-thisCtx.Done():
							return
						case dataChan <- dM:
							continue
						}
					}
				}
			}
		}
	}
}

func (s *grpcRemoteServer) GetRequestMetadata(
	ctx context.Context, uri ...string) (
	map[string]string, error) {

	username, pw, err := s.auth.getUP()
	return map[string]string{
		"username": username,
		"password": pw,
	}, err
}

func (s *grpcRemoteServer) RequireTransportSecurity() bool {
	//
	// We should make this a configurable option
	return false
}

//
// Sticky loop when we are handling a remote server, involves trying
// to stay connected and pulling streams for all the subscriptions.
func (server *grpcRemoteServer) stickyLoop(ctx context.Context) {

	var codecType encoding

	c := server.common
	logctx := c.logctx

	//
	// We need to decide what form of telemetry encap to request, and
	// what codec to use.
	//
	// Request whatever has been configured
	err, requestGRPCEncoding := mapEncapToGRPCEncode(c.encodingRequest)
	if err != nil {
		logctx.WithError(err).Error(GRPCLOGPRE +
			"Subscriptions aborted, fatal, no retry")
		return
	}

	codecType = c.encap
	err, codec := getCodec(c.name, codecType)
	if err != nil {
		logctx.WithError(err).Error(GRPCLOGPRE +
			"Subscriptions aborted, fatal, no retry")
		return
	}

	//
	// Prepare dial options (TLS, user/password, timeout...)
	var opts []grpc.DialOption
	if c.tls {

		var creds credentials.TransportCredentials
		var err error
		if c.tls_pem != "" {
			creds, err = credentials.NewClientTLSFromFile(
				c.tls_pem,
				c.tls_servername)
			if err != nil {
				logctx.WithError(err).Error(
					GRPCLOGPRE +
						"Subscriptions aborted, " +
						"loading TLS PEM, fatal, " +
						"no retry")
				return
			}
		} else {
			creds = credentials.NewClientTLSFromCert(
				nil, c.tls_servername)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))

	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	opts = append(opts, grpc.WithPerRPCCredentials(server))
	opts = append(opts,
		grpc.WithTimeout(time.Second*time.Duration(GRPC_TIMEOUT)))

	conn, err := grpc.Dial(server.server, opts...)
	if err != nil {
		logctx.WithError(err).Error(GRPCLOGPRE + "Dial failed, retrying")
		//
		// THIS IS IMPORTANT.
		// We simply close the childrenDone channel to retry.
		close(c.childrenDone)
		return
	}
	defer conn.Close()

	logctx.WithFields(log.Fields{
		"codec": encodingToName(codecType)}).Info(
		GRPCLOGPRE + "Connected")

	var wg sync.WaitGroup
	for _, sub := range server.subscriptions {

		args := dialin.CreateSubsArgs{
			ReqId:    server.reqID,
			Encode:   requestGRPCEncoding,
			Subidstr: sub,
		}
		server.reqID = server.reqID + 1

		//
		// Setup client on connection
		client := dialin.NewGRPCConfigOperClient(conn)
		stream, err := client.CreateSubs(ctx, &args)
		if err != nil {
			logctx.WithError(err).WithFields(
				log.Fields{"subscription": sub}).
				Error(GRPCLOGPRE + "Subscription setup failed")
			continue
		}

		// Subscription setup, kick off go routine to handle
		// the stream. Add child to wait group such that this
		// routine can wait for all its children on the way
		// out.
		wg.Add(1)
		go singleSubscription(
			ctx, server, sub, server.reqID,
			stream, codec, c.dataChans, &wg, logctx, c.logData)
	}

	wg.Wait()
	logctx.Info(GRPCLOGPRE + "All subscriptions closed")
	close(c.childrenDone)

	return
}

///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////
///////             D I A L O U T    H A N D L E R              ///////
///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////

type grpcLocalServer struct {
	server      string
	codec       codec
	control_ctx context.Context
	common      *grpcBlock
}

func (s *grpcLocalServer) String() string {
	return s.server
}

func (s *grpcLocalServer) ExtendLogContext(logctx *log.Entry) *log.Entry {
	return logctx.WithFields(log.Fields{
		"type":  "pipeline is SERVER",
		"encap": encodingToName(s.common.encap),
	})
}

func (s *grpcLocalServer) linkServerBlock(c *grpcBlock) {
	s.common = c
}

type dummyPeerType struct{}

func (d *dummyPeerType) String() string {
	return "Unknown addr"
}

func (d *dummyPeerType) Network() string {
	return "Unknown net"
}

var dummyPeer dummyPeerType

//
// We could probably refactor the guts of singleSubscription from
// dialin, and share it with dialout
func (s *grpcLocalServer) MdtDialout(
	stream dialout.GRPCMdtDialout_MdtDialoutServer) error {

	var endpoint *peer.Peer
	var ok bool

	logctx := s.common.logctx

	if endpoint, ok = peer.FromContext(stream.Context()); !ok {
		endpoint = &peer.Peer{
			Addr: &dummyPeer,
		}
	}

	logctx.WithFields(
		log.Fields{
			"peer": endpoint.Addr.String(),
		}).Info(GRPCLOGPRE + "Receiving dialout stream")

	for {
		//
		// Recv will error out if server is stopped so we do not
		// have to handle local context explicitly
		reply, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				logctx.WithError(err).Error(GRPCLOGPRE +
					"session closed")
			} else {
				logctx.WithError(err).Error(GRPCLOGPRE +
					"session error")
			}
			return err
		}

		if len(reply.Data) == 0 {
			if len(reply.Errors) != 0 {
				logctx.WithFields(log.Fields{
					"errmsg": reply.Errors,
					"peer":   endpoint.Addr.String(),
				}).Error(GRPCLOGPRE + "error from client")
				return nil
			}
		}

		if s.common.logData {
			logctx.WithFields(log.Fields{
				"reqID": reply.ReqId,
				"peer":  endpoint.Addr.String(),
				"msg":   hex.Dump(reply.Data),
			}).Debug("gRPC server logdata")
		}

		err, dMs := s.codec.blockToDataMsgs(endpoint.Addr, reply.Data)
		if err != nil {
			logctx.WithFields(log.Fields{
				"reqID": reply.ReqId,
				"peer":  endpoint.Addr.String(),
			}).WithError(err).Error(
				GRPCLOGPRE + "Extracting msg from stream failed")
			return err
		}

		if len(dMs) == 0 {
			logctx.WithFields(log.Fields{
				"reqID": reply.ReqId,
				"peer":  endpoint.Addr.String(),
			}).Error(
				GRPCLOGPRE + "Extracting msg from stream came up empty")
		}

		if dMs != nil {
			for _, dM := range dMs {
				//
				// Push data onto channel.
				for _, dataChan := range s.common.dataChans {
					//
					// Make sure that if we are blocked on consumer,
					// we still handle cancel.
					select {
					case <-s.control_ctx.Done():
						logctx.WithFields(
							log.Fields{
								"peer": endpoint.Addr.String(),
							}).Info(GRPCLOGPRE + "Closed from pipeline end")
						return nil
					case dataChan <- dM:
						continue
					}
				}
			}
		}
	}
}

func (server *grpcLocalServer) stickyLoop(ctx context.Context) {

	var err error
	c := server.common
	logctx := c.logctx

	err, server.codec = getCodec(c.name, c.encap)
	if err != nil {
		logctx.WithError(err).Error(GRPCLOGPRE +
			"Failed to retrieve pertinent codec, fatal, no retry")
		return
	}

	server.control_ctx = ctx

	var opts []grpc.ServerOption
	if c.tls {
		creds, err := credentials.NewServerTLSFromFile(c.tls_pem, c.tls_key)
		if err != nil {
			logctx.WithFields(log.Fields{
				"tls_pem": c.tls_pem,
				"tls_key": c.tls_key,
			}).WithError(err).Error(GRPCLOGPRE +
				"TLS requested but failed setup, fatal, no retry")
			return
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}

	grpcServer := grpc.NewServer(opts...)
	dialout.RegisterGRPCMdtDialoutServer(grpcServer, server)

	lis, err := net.Listen("tcp", server.server)
	if err != nil {
		// We simply close the childrenDone channel to retry.
		logctx.WithError(err).Error(GRPCLOGPRE + "Listen failed, retrying")
		close(c.childrenDone)
		return
	}

	acceptFailed := make(chan struct{})
	go func() {
		//
		// Serve returns if accept failed.
		logctx.Info(GRPCLOGPRE + "Start accepting dialout sessions")
		grpcServer.Serve(lis)
		close(acceptFailed)
	}()

	//
	// Caller can cancel (e.g. if we are shutting down section), or
	// accept may fail. In either case, we will stop the server,
	// indicate we're going and return.  Caller will decide whether we
	// get to come again or not.
	//
	select {
	case <-ctx.Done():
		//
		// Serve returns if accept failed.
		logctx.Info(GRPCLOGPRE + "Stop accepting dialout sessions")
	case <-acceptFailed:
		//
		// Serve returns if accept failed.
		logctx.Info(
			GRPCLOGPRE + "Stop accepting dialout sessions, accept failed")
	}

	grpcServer.Stop()

	close(c.childrenDone)
	return
}

///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////
///////                        C O M M O N                      ///////
///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////

//
// Run the client/server until shutdown. The role of this outer
// handler is to receive control plane messages and convey them to
// workers, as well as to retry sticky loops if we bail.
// Note: this loop handle dialout and dialin roles.
func (c *grpcBlock) run() {

	var stats msgStats
	var cancel context.CancelFunc
	var ctx context.Context

	logctx := logger.WithFields(log.Fields{
		"name":   c.name,
		"server": c.server,
		"encap":  encodingToName(c.encap),
	})

	logctx = c.server.ExtendLogContext(logctx)
	c.logctx = logctx

	go func() {
		// Prime the pump, and kick off the first retry.
		close(c.childrenDone)
	}()

	logctx.Info("gRPC starting block")

	for {
		select {

		case <-c.childrenDone:

			//
			// If we receive childrenDone signal here, we need to retry.
			// Start by making a new channel.
			ctx, cancel = context.WithCancel(context.Background())
			c.childrenDone = make(chan struct{})
			go c.server.stickyLoop(ctx)
			//
			// Ensure that if we bailed out right away we
			// do not reschedule immediately.
			time.Sleep(
				XPORT_GRPC_WAIT_TO_REDIAL * time.Second)

		case msg := <-c.ctrlChan:
			switch msg.id {
			case REPORT:
				content, _ := json.Marshal(stats)
				resp := &ctrlMsg{
					id:       ACK,
					content:  content,
					respChan: nil,
				}
				msg.respChan <- resp

			case SHUTDOWN:
				logctx.WithFields(
					log.Fields{
						"ctrl_msg_id": msg.id,
					}).Info(GRPCLOGPRE +
					"gRPC client loop, " +
					"rxed SHUTDOWN, closing connections")

				//
				// Flag cancellation of binding and
				// its connections and wait for
				// cancellation to complete
				// synchronously.
				if cancel != nil {
					logctx.Debug(GRPCLOGPRE +
						"waiting for children")
					cancel()
					<-c.childrenDone
				} else {
					logctx.Debug(GRPCLOGPRE +
						"NOT waiting for children")
				}

				logctx.Debug(GRPCLOGPRE + "gRPC server notify " +
					"conductor binding is closed")
				resp := &ctrlMsg{
					id:       ACK,
					respChan: nil,
				}
				msg.respChan <- resp
				return

			default:
				logctx.Error(GRPCLOGPRE +
					"gRPC block loop, unknown ctrl message")
			}
		}
	}
}

// Module implement inputNodeModule interface
type grpcInputModule struct {
}

func grpcInputModuleNew() inputNodeModule {
	return &grpcInputModule{}
}

//
// extractSubscriptions is a helper used to extract subscription array
// from config string
func extractSubscriptions(subcfg string) []string {

	subscriptions := strings.Split(subcfg, ",")
	cleanSubs := make([]string, 0)
	for _, sub := range subscriptions {
		cleanSubs = append(cleanSubs, strings.TrimSpace(sub))
	}

	return cleanSubs
}

func (m *grpcInputModule) configureRemoteServer(
	name string,
	serverSockAddr string,
	nc nodeConfig) (error, *grpcRemoteServer) {

	subcfg, err := nc.config.GetString(name, "subscriptions")
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{"name": name}).Error(
			"'subscriptions' must be specified in this section")
		return err, nil
	}
	cleanSubs := extractSubscriptions(subcfg)

	//
	// Handle user/password
	authCollect := grpcUPCollectorFactory()
	err = authCollect.handleConfig(nc, name, serverSockAddr)
	if err != nil {
		return err, nil
	}

	//
	// Make sure we were successful, one way or another.
	server := &grpcRemoteServer{
		server:        serverSockAddr,
		subscriptions: cleanSubs,
		reqID:         rand.Int63(),
		auth:          authCollect,
	}

	return nil, server
}

func (m *grpcInputModule) configureLocalServer(
	name string,
	serverSockAddr string,
	nc nodeConfig) (error, *grpcLocalServer) {

	server := &grpcLocalServer{
		server: serverSockAddr,
	}

	return nil, server
}

//
// Read the module configuration, and set us up to handle control
// requests, receive telemetry and feed the data channels passed in.
func (m *grpcInputModule) configure(
	name string,
	nc nodeConfig,
	dataChans []chan<- dataMsg) (error, chan<- *ctrlMsg) {

	var server grpcServer
	var serverSockAddr string
	var encodingRequest encoding
	var err error

	//
	// Set up pseudo random seed for ReqID generation. Nothing
	// secret and pseudo random works just fine here.
	rand.Seed(time.Now().UnixNano())

	//
	// If not set, will default to false
	logData, _ := nc.config.GetBool(name, "logdata")

	//
	// If not set, will default to false
	tls, _ := nc.config.GetBool(name, "tls")
	tls_pem, _ := nc.config.GetString(name, "tls_pem")
	tls_servername, _ := nc.config.GetString(name, "tls_servername")
	tls_key, _ := nc.config.GetString(name, "tls_key")

	encoding, _ := nc.config.GetString(name, "encoding")
	encap, _ := nc.config.GetString(name, "encap")
	if encap == "" {
		encap = "gpb"
	}

	if serverSockAddr, err = nc.config.GetString(name, "server"); err == nil {

		if encoding == "" {
			// Default to gpbkv if not specified.
			encoding = "gpbkv"
		}
		if encoding == "gpb" {
			// Interpret gpb as gpbcompact
			encoding = "gpbcompact"
		}
		//
		// Make sure encap and encoding are compatible.
		if encap != "gpb" && encap != encoding {
			err = fmt.Errorf(
				"grpc misconfiguration, 'encap' and 'encoding' do not match")
			logger.WithError(err).WithFields(
				log.Fields{
					"name":     name,
					"encap":    encap,
					"encoding": encoding,
				}).Error(
				"grpc encap/encoding mismatch")
			return err, nil
		}

		err, encodingRequest = nameToEncoding(encoding)
		if err != nil {
			logger.WithError(err).WithFields(
				log.Fields{"name": name}).Error(
				"'encoding' option for gRPC input")
			return err, nil
		}
		err, server = m.configureRemoteServer(name, serverSockAddr, nc)
	} else if serverSockAddr, err =
		nc.config.GetString(name, "listen"); err == nil {
		//
		// Encoding is not relevant for dialout
		if encoding != "" {
			logger.WithError(err).WithFields(
				log.Fields{"name": name}).Error(
				"'encoding' ignored on dialout, encoding is identfiable on the wire")
		}

		err, server = m.configureLocalServer(name, serverSockAddr, nc)
	} else {
		err = fmt.Errorf("attribute 'server' (dialin - pipeline2router connection) or  " +
			"'listen' (dialout - router2pipeline connection)  must be " +
			"specified for gRPC client")
	}

	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{"name": name}).Error("Failed to setup gRPC input server")
		return err, nil

	}

	err, streamType := nameToEncoding(encap)
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{"name": name,
				"encap": encap}).Error(
			"'encap' option for gRPC input")
		return err, nil
	}

	//
	// Create a control channel which will be used to control us,
	// and kick off the server which will accept connections and
	// listen for control requests.
	ctrlChan := make(chan *ctrlMsg)

	block := &grpcBlock{
		name:            name,
		server:          server,
		encap:           streamType,
		encodingRequest: encodingRequest,
		logData:         logData,
		ctrlChan:        ctrlChan,
		dataChans:       dataChans,
		childrenDone:    make(chan struct{}),
		tls:             tls,
		tls_pem:         tls_pem,
		tls_servername:  tls_servername,
		tls_key:         tls_key,
	}

	//
	// Set up link from specific server handler to common block
	server.linkServerBlock(block)

	go block.run()

	return nil, ctrlChan
}

var grpcUPCollectorFactory userPasswordCollectorFactory

//
// We use init to setup the default user/password collection
// factory. This can be overwritten by test.
func init() {
	grpcUPCollectorFactory = func() userPasswordCollector {
		return &cryptUserPasswordCollector{}
	}
}
