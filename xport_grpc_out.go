//
// This module provides for dialin from downstream clients wishing to
// consume anything handled by the pipeline over a grpc session.

package main

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"net"
	"sync"
	"time"
)

const (
	// Maximum time in seconds to wait when draining queues on
	// shutdown
	DRAINTIMEOUT = 5
)

//
// Configuration block represents desired configuration of server.
type grpcOutLocalServerConfig struct {
	name string
	//
	// Server socket to listen on
	server string
	//
	// Desired output stream spec
	streamSpec *dataMsgStreamSpec

	//
	// TLS configuration
	tls            bool
	tls_pem        string
	tls_key        string
	tls_servername string
	//
	// Channel depth
	dataChannelDepth int
	//
	// Logging?
	logData bool
}

type grpcOutLocalServer struct {
	cfg *grpcOutLocalServerConfig
	//
	// Control channel used to control the gRPC server
	ctrlChan <-chan *ctrlMsg
	//
	// Data channels feeding output
	dataChan <-chan dataMsg
	//
	// wait group used to wait on children draining and shutting down
	childrenWG sync.WaitGroup
	//
	// Used to signal that all children have shutdown up to outer
	// loop
	childrenDone chan struct{}
	//
	// Setup a consistent log entry to build logging from
	logctx *log.Entry
	//
	// Channels to the child connections
	childrenMutex sync.Mutex
	childrenIndex uint64
	children      map[uint64]*grpcOutConnToClient
}

func grpcOutLocalServerFromConfig(
	cfg *grpcOutLocalServerConfig) *grpcOutLocalServer {

	s := &grpcOutLocalServer{cfg: cfg}

	s.childrenDone = make(chan struct{})
	s.children = make(map[uint64]*grpcOutConnToClient)

	return s
}

type grpcOutConnToClient struct {
	endpoint *peer.Peer
	dataChan chan *[]byte
}

//
// Add a channel
func (s *grpcOutLocalServer) addChild(client *grpcOutConnToClient) uint64 {
	s.childrenMutex.Lock()
	defer s.childrenMutex.Unlock()
	s.childrenWG.Add(1)
	s.childrenIndex++
	s.children[s.childrenIndex] = client
	return s.childrenIndex
}

//
// removeChildUnprotected removes child expecting caller
// to have set up mutex protection
func (s *grpcOutLocalServer) removeChildUnprotected(index uint64) {
	c, ok := s.children[index]
	if ok {
		// delete is a noop if index did not exist, but we are in the
		// if ok part because we also want to close the channel
		delete(s.children, index)
		close(c.dataChan)
	}
}

//
// Remove a channel
func (s *grpcOutLocalServer) removeChild(index uint64) {
	s.childrenMutex.Lock()
	defer s.childrenMutex.Unlock()
	s.removeChildUnprotected(index)
}

//
// Remove all channels
func (s *grpcOutLocalServer) removeAllChildren() {
	s.childrenMutex.Lock()
	defer s.childrenMutex.Unlock()
	for k, _ := range s.children {
		s.removeChildUnprotected(k)
	}
}

//
// Remove all channels
func (s *grpcOutLocalServer) feedAllChildren(data *[]byte) {
	s.childrenMutex.Lock()
	defer s.childrenMutex.Unlock()
	for _, v := range s.children {
		if len(v.dataChan) == cap(v.dataChan) {
			// Drop and count to avoid head of line blocking
			grpcOutMetaMonitor.CountersDrops.WithLabelValues(
				s.cfg.name, v.endpoint.Addr.String()).Inc()
		} else {
			v.dataChan <- data
			grpcOutMetaMonitor.CountersMsgs.WithLabelValues(
				s.cfg.name, v.endpoint.Addr.String()).Inc()
			grpcOutMetaMonitor.CountersBytes.WithLabelValues(
				s.cfg.name, v.endpoint.Addr.String()).Add(
				float64(len(*data)))
		}
	}
}

//
// Conduit to connected client.
func (s *grpcOutLocalServer) Pull(req *SubJSONReqMsg, gs GRPCOut_PullServer) error {

	var endpoint *peer.Peer
	var err error
	var ok bool

	streamCtx := gs.Context()
	if endpoint, ok = peer.FromContext(streamCtx); !ok {
		endpoint = &peer.Peer{
			Addr: &dummyPeer,
		}
	}

	rep := &SubJSONRepMsg{
		ReqId: req.ReqId,
	}

	msgChan := make(chan *[]byte, s.cfg.dataChannelDepth)
	childId := s.addChild(&grpcOutConnToClient{
		endpoint: endpoint,
		dataChan: msgChan,
	})

	s.logctx.WithFields(
		log.Fields{
			"ReqId":   req.ReqId,
			"ChildId": childId,
			"peer":    endpoint.Addr.String(),
		}).Info("Downstream connection pulling")

	//
	// Loop through waiting for messages from upstream, close or
	// error.
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				s.logctx.WithFields(
					log.Fields{
						"peer": endpoint.Addr.String(),
					}).Info("Upstream channel closed")
				goto out
			}
			//
			// Handle collected message
			rep.Data = *msg

			if s.cfg.logData {
				s.logctx.WithFields(
					log.Fields{
						"peer":  endpoint.Addr.String(),
						"reply": rep,
					}).Debug("sending")
			}
			err = gs.Send(rep)
			if err != nil {
				grpcOutMetaMonitor.CountersErrors.WithLabelValues(
					s.cfg.name, endpoint.Addr.String()).Inc()
				s.logctx.WithError(err).WithFields(
					log.Fields{
						"peer": endpoint.Addr.String(),
					}).Error("Failing to send to peer")
				goto out
			}
		case <-streamCtx.Done():
			s.logctx.WithFields(
				log.Fields{
					"peer": endpoint.Addr.String(),
				}).Info("Client cancelled stream")
			goto out
		}
	}

out:
	s.removeChild(childId) // if necessary
	s.childrenWG.Done()

	s.logctx.WithFields(
		log.Fields{
			"peer": endpoint.Addr.String(),
		}).Info("Quit client stream")

	return nil
}

func (s *grpcOutLocalServer) stickyLoop(
	ctx context.Context) {

	var opts []grpc.ServerOption
	if s.cfg.tls {
		creds, err := credentials.NewServerTLSFromFile(
			s.cfg.tls_pem, s.cfg.tls_key)
		if err != nil {
			s.logctx.WithFields(log.Fields{
				"tls_pem": s.cfg.tls_pem,
				"tls_key": s.cfg.tls_key,
			}).WithError(err).Error(GRPCLOGPRE +
				"TLS configured but failed setup, fatal, no retry")
			return
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}

	grpcServer := grpc.NewServer(opts...)
	RegisterGRPCOutServer(grpcServer, s)

	lis, err := net.Listen("tcp", s.cfg.server)
	if err != nil {
		// We simply close the childrenDone channel to retry.
		s.logctx.WithError(err).Error(
			"Listen for downstream sessions failed, retrying")
		close(s.childrenDone)
		return
	}

	acceptFailed := make(chan struct{})
	go func() {
		//
		// Serve returns if accept failed.
		s.logctx.Info("Start accepting downstream sessions")
		grpcServer.Serve(lis)
		close(acceptFailed)
	}()

	//
	// Caller can cancel (e.g. if we are shutting down section), or
	// accept may fail. In either case, we will stop the server,
	// indicate we're going and return.  Caller will decide whether we
	// get to come again or not.
	//
	for {
		select {
		case dM, ok := <-s.dataChan:
			//
			// Handle messages and then broadcast to downstream children
			// to send out
			if !ok {
				// Channel has been closed. Our demise is near. SHUTDOWN
				// is likely to be received soon on control channel. While
				// the only message we receive is shutdown, this could
				// have done without a control channel and instead should
				// have used range over the channel.
				//
				s.dataChan = nil
				continue
			}
			if s.cfg.logData {
				s.logctx.WithFields(
					log.Fields{
						"peer": "common",
						"msg":  dM.getDataMsgDescription(),
					}).Debug("collected message from upstream")
			}

			err, rawstream := dM.produceByteStream(s.cfg.streamSpec)
			if err != nil {
				s.logctx.WithError(err).WithFields(log.Fields{
					"msg": dM.getDataMsgDescription(),
				}).Error("gRPC out feeder loop, failed message")
			} else {
				s.feedAllChildren(&rawstream)
			}

		case <-ctx.Done():
			//
			// Serve returns if accept failed.
			s.logctx.Info("Stop accepting downstream sessions (from conductor)")
			goto out

		case <-acceptFailed:
			//
			// Serve returns if accept failed.
			s.logctx.Info("Stop accepting downstream sessions (accept failed)")
			goto out
		}
	}

out:

	s.removeAllChildren()

	waitDone := make(chan struct{})
	waitTimedOut := make(chan struct{})
	go func() {
		defer close(waitDone)
		s.childrenWG.Wait()
	}()
	go func() {
		defer close(waitTimedOut)
		time.Sleep(time.Second * DRAINTIMEOUT)
		// could have used timer, but want to close channel
	}()

	//
	// Block waiting for all children to drain, or on timeout
	select {
	case <-waitTimedOut:
	case <-waitDone:
	}

	grpcServer.Stop()

	//
	// Signal we are done to outermost
	close(s.childrenDone)
	return
}

func (s *grpcOutLocalServer) outermostLoop(
	dataChan <-chan dataMsg,
	ctrlChan <-chan *ctrlMsg) {

	var cancel context.CancelFunc
	var ctx context.Context

	s.logctx = logger.WithFields(log.Fields{
		"name":       s.cfg.name,
		"server":     s.cfg.server,
		"streamSpec": s.cfg.streamSpec,
	})

	s.dataChan = dataChan
	s.ctrlChan = ctrlChan

	go func() {
		// Prime the pump, and kick off the first retry.
		close(s.childrenDone)
	}()

	s.logctx.Info("gRPC out starting block")

	for {

		select {

		case <-s.childrenDone:
			//
			// If we receive childrenDone signal here, we need to retry.
			// Start by making a new channel.
			ctx, cancel = context.WithCancel(context.Background())
			s.childrenDone = make(chan struct{})
			go s.stickyLoop(ctx)
			//
			// Ensure that if we bailed out right away we
			// do not reschedule immediately.
			time.Sleep(XPORT_GRPC_WAIT_TO_REDIAL * time.Second)

		case msg := <-s.ctrlChan:
			switch msg.id {
			case REPORT:
				//
				// stats need cleaning up, we should be using prom
				// scraping alone at this point
				var stats msgStats
				content, _ := json.Marshal(stats)
				resp := &ctrlMsg{
					id:       ACK,
					content:  content,
					respChan: nil,
				}
				msg.respChan <- resp

			case SHUTDOWN:
				s.logctx.WithFields(
					log.Fields{
						"ctrl_msg_id": msg.id,
					}).Info("gRPC out loop, " +
					"rxed SHUTDOWN, closing connections")

				//
				// Flag cancellation of binding and its connections
				// and wait for cancellation to complete
				// synchronously.
				if cancel != nil {
					s.logctx.Debug("gRPC out waiting for children")
					cancel()
					<-s.childrenDone
				} else {
					s.logctx.Debug("grpc out NOT waiting for children")
				}

				s.logctx.Debug("gRPC server notify conductor binding is closed")
				resp := &ctrlMsg{
					id:       ACK,
					respChan: nil,
				}
				msg.respChan <- resp
				return

			default:
				s.logctx.Error("gRPC out block loop, unknown ctrl message")
			}
		}
	}
}

type grpcOutputModule struct{}

func grpcOutputModuleNew() outputNodeModule {
	return &grpcOutputModule{}
}

func (m *grpcOutputModule) configure(
	name string,
	nc nodeConfig) (error, chan<- dataMsg, chan<- *ctrlMsg) {

	var cfg grpcOutLocalServerConfig

	cfg.name = name

	server, err := nc.config.GetString(name, "listen")
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{
				"name": name,
			}).Error("'listen' option required for gRPC output")
		return err, nil, nil
	}
	cfg.server = server

	err, cfg.streamSpec = dataMsgStreamSpecFromConfig(nc, name)
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{
				"name": name,
			}).Error("'encoding' option for gRPC output")
		return err, nil, nil
	}

	cfg.logData, _ = nc.config.GetBool(name, "logdata")

	cfg.tls, _ = nc.config.GetBool(name, "tls")
	cfg.tls_pem, _ = nc.config.GetString(name, "tls_pem")
	cfg.tls_servername, _ = nc.config.GetString(name, "tls_servername")
	cfg.tls_key, _ = nc.config.GetString(name, "tls_key")

	cfg.dataChannelDepth, err = nc.config.GetInt(name, "datachanneldepth")
	if err != nil {
		cfg.dataChannelDepth = DATACHANNELDEPTH
	}

	ctrlChan := make(chan *ctrlMsg)
	dataChan := make(chan dataMsg, cfg.dataChannelDepth)

	// Kick off the real action. Get a server, and run it.
	s := grpcOutLocalServerFromConfig(&cfg)
	go s.outermostLoop(dataChan, ctrlChan)

	return nil, dataChan, ctrlChan
}

type grpcOutMetaMonitorType struct {
	CountersMsgs   *prometheus.CounterVec
	CountersBytes  *prometheus.CounterVec
	CountersErrors *prometheus.CounterVec
	CountersDrops  *prometheus.CounterVec
}

var grpcOutMetaMonitor *grpcOutMetaMonitorType

func init() {
	grpcOutMetaMonitor = &grpcOutMetaMonitorType{
		CountersMsgs: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "grpcOut_messages",
				Help: "Messages",
			},
			[]string{"section", "peer"}),
		CountersBytes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "grpcOut_bytes",
				Help: "Bytes",
			},
			[]string{"section", "peer"}),
		CountersErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "grpcOut_errors",
				Help: "Errors",
			},
			[]string{"section", "peer"}),
		CountersDrops: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "grpcOut_drops",
				Help: "Drops",
			},
			[]string{"section", "peer"}),
	}

	// Dump content
	prometheus.MustRegister(grpcOutMetaMonitor.CountersMsgs)
	prometheus.MustRegister(grpcOutMetaMonitor.CountersBytes)
	prometheus.MustRegister(grpcOutMetaMonitor.CountersErrors)
	prometheus.MustRegister(grpcOutMetaMonitor.CountersDrops)
}
