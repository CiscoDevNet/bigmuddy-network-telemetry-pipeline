//
// January 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Handle TCP transports

package main

import (
	"encoding/hex"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
	"time"
)

const (
	//
	// Time to wait before attempting to accept connection.
	XPORT_TCP_WAIT_TO_REBIND = 1
)

type serverTCP struct {
	name      string
	bindpoint string
	encap     string
	//
	// Log data into debug log
	logData bool
	//
	// TCP keepalive period in second. 0 stops pipeline from enabling
	// it.
	keepaliveSeconds time.Duration
	// Listener over which connections are accepted.
	listener net.Listener
	// Control channel used to control the server
	ctrlChan <-chan *ctrlMsg
	// Data channels fed by the server
	dataChans []chan<- dataMsg
	//
	// Wait group used to synchronise shutdown of all open
	// connection
	connectionGroup *sync.WaitGroup

	//
	// Channel used to signal up to conductor that we are done
	doneCancel chan int
	//
	// Channel used withing the server to request connections come
	// down
	cancelConn chan struct{}
}

func (s *serverTCP) cancelled() bool {
	select {
	case <-s.cancelConn:
		return true
	default:
		// Nothing to do, go
		// back sound the
		// loop.
	}

	return false
}

func (s *serverTCP) handleConnection(conn net.Conn) {

	logctx := logger.WithFields(
		log.Fields{
			"name":      s.name,
			"local":     conn.LocalAddr().String(),
			"remote":    conn.RemoteAddr().String(),
			"encap":     s.encap,
			"keepalive": s.keepaliveSeconds,
		})
	logctx.Info("TCP server accepted connection")

	defer func() {
		// Log, close the connection, and indicate to the wait
		// group that we are done.
		logctx.Info("TCP server closing connection")
		conn.Close()
		s.connectionGroup.Done()
	}()

	//
	// Fetch a parser of the appropriate type.
	err, parser := getNewEncapParser(s.name, s.encap, conn.RemoteAddr())
	if err != nil {
		logctx.WithError(err).Error("TCP server failed to fetch parser")
		return
	}

	// Make buffered channels which will allow us to handle
	// cancellation while waiting to read from socket.
	//
	// Anonymous function reading from socket will signal back over
	// error or result channels, unless operation is cancelled
	// first.
	//
	// A buffer length of one is sufficient; it is only used to
	// decouple reading from handler. A vain early optimisation is to
	// reuse the channel across the iterations reading from socket
	// rather than adding a channel every time; hence the use of the
	// buffered channel as opposed to using the pattern of simply
	// closing channel as done signal.
	readDone := make(chan int, 1)
	readErr := make(chan error, 1)

	//
	// Setup TCP Keepalive. On linux, confirm using:
	//	ss -n -i -t -o '( sport = :<listenport> )'
	// State       Recv-Q Send-Q   ... Address:Port
	// ESTAB       0      0        ...:62436  timer:(keepalive,8min58sec,0)
	//
	if s.keepaliveSeconds != 0 {
		err = conn.(*net.TCPConn).SetKeepAlive(true)
		if err != nil {
			logctx.WithError(err).Error("TCP keepalive setup failed")
		} else {
			err = conn.(*net.TCPConn).SetKeepAlivePeriod(s.keepaliveSeconds)
			if err != nil {
				logctx.WithError(err).Error("TCP keepalive period setup failed")
			}
		}
	}

	for {

		// Get the next buffer expected by the parser
		err, buffer := parser.nextBlockBuffer()
		if err != nil || len(*buffer) == 0 {
			logger.WithError(err).Error("TCP server failed to fetch buffer")
			return
		}

		//
		// What are we waiting for:
		if s.logData {
			logger.WithFields(log.Fields{
				"len": len(*buffer),
			}).Debug("waiting to read 'len' from socket")
		}

		go func() {
			n, err := io.ReadFull(conn, *buffer)
			if err != nil {
				readErr <- err
			} else {
				readDone <- n
			}
		}()

		select {
		case <-s.cancelConn:
			//
			// Shutting down?
			logger.Info("Cancelled connection")
			return
		case err = <-readErr:
			//
			// Read failed
			logger.WithError(err).Error("TCP server failed on read full")
			return
		case <-readDone:
			//
			// We're done reading what we expected
		}

		//
		// We got the data we were expecting
		err, msgs := parser.nextBlock(*buffer, nil)
		if err != nil {
			logger.WithError(err).WithFields(
				log.Fields{
					"len": len(*buffer),
					"msg": hex.Dump(*buffer),
				}).Error("Failed to extract next buffer")
			return
		}

		if s.logData {
			logger.WithFields(log.Fields{
				"dataMsgCount": len(msgs),
				"len":          len(*buffer),
				"msg":          hex.Dump(*buffer),
			}).Debug("TCP server logdata")
		}

		//
		// It is perfectly valid for there not
		// to be a message to send on; e.g. we
		// have just read and on the wire
		// header which simply updates parser
		// state.
		if msgs == nil {
			continue
		}

		//
		// Spray the generated messages across each
		// available downstream channel
		//
		for _, msg := range msgs {
			for _, dataChan := range s.dataChans {
				dataChan <- msg
			}
		}
	}
}

func (s *serverTCP) acceptTCPConnections() {

	defer func() {

		// Wait for exit of cancelled connections if necessary.
		logger.WithFields(log.Fields{
			"name":      s.name,
			"bindpoint": s.bindpoint,
		}).Debug("TCP server waiting for connections to cancel")
		s.connectionGroup.Wait()

		logger.WithFields(log.Fields{
			"name":      s.name,
			"bindpoint": s.bindpoint,
		}).Debug("TCP server destroying binding")

		//
		// Tell top half we're done cleaning up
		s.doneCancel <- 1

		// Canceller will ensure server is removed from
		// serversTCP with something like this:
		// delete(serversTCP, s.bindpoint)
	}()

	for {
		conn, err := s.listener.Accept()
		if s.cancelled() {
			//
			// We may be here because the northbound
			// cancelled on us (and called Close)
			logger.WithFields(log.Fields{
				"name":      s.name,
				"bindpoint": s.bindpoint,
			}).Debug("TCP server cancel binding")
			return
		}

		if err != nil {
			logger.WithError(err).WithFields(
				log.Fields{
					"name":      s.name,
					"bindpoint": s.bindpoint,
				}).Error("TCP connection accept failed")
			// We keep trying, but use a retry
			// timeout. Note that when we're in this
			// sleep, we will also not be handling
			// deletes.
			time.Sleep(
				XPORT_TCP_WAIT_TO_REBIND * time.Second)
		}

		//
		// Look for cancellation from controller.
		s.connectionGroup.Add(1)
		go s.handleConnection(conn)
	}

}

func (s *serverTCP) startServer() {

	var stats msgStats

	logger.WithFields(log.Fields{
		"name":   s.name,
		"listen": s.bindpoint}).Info("TCP server starting")

	//
	// Start accepting connections
	go s.acceptTCPConnections()

	for {
		select {

		case msg := <-s.ctrlChan:
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
				logger.WithFields(
					log.Fields{"name": s.name}).Info(
					"TCP server loop, rxed SHUTDOWN, closing connections")

				//
				// Flag cancellation of binding and
				// its connections and wait for
				// cancellation to complete
				// synchronously.
				close(s.cancelConn)
				s.listener.Close()

				logger.WithFields(
					log.Fields{
						"name":      s.name,
						"bindpoint": s.bindpoint,
					}).Info("TCP server notify conductor binding is closed")

				resp := &ctrlMsg{
					id:       ACK,
					respChan: nil,
				}
				msg.respChan <- resp
				return

			default:
				logger.WithFields(
					log.Fields{"name": s.name}).Error(
					"TCP server loop, unknown ctrl message")
			}
		}
	}

}

//
// addTCPServer adds the new service to serversTCP if necessary.
// Runs in the context of the conductor handler
func addTCPServer(
	name string,
	bindpoint string,
	encap string,
	dataChans []chan<- dataMsg,
	ctrlChan <-chan *ctrlMsg,
	keepalive int,
	logData bool) error {

	listener, err := net.Listen("tcp", bindpoint)
	if err != nil {
		logger.WithError(err).WithFields(log.Fields{
			"name":      name,
			"bindpoint": bindpoint,
		}).Error("TCP server failed to bind")
		return err
	}

	s := new(serverTCP)
	s.name = name
	s.listener = listener
	s.bindpoint = bindpoint
	s.encap = encap
	s.logData = logData
	s.keepaliveSeconds = time.Duration(keepalive) * time.Second
	s.dataChans = dataChans
	s.ctrlChan = ctrlChan
	s.cancelConn = make(chan struct{})
	s.doneCancel = make(chan int)
	s.connectionGroup = new(sync.WaitGroup)

	go s.startServer()

	return nil
}

// Module implement inputNodeModule interface
type tcpInputModule struct {
}

func tcpInputModuleNew() inputNodeModule {
	return &tcpInputModule{}
}

func (m *tcpInputModule) configure(
	name string,
	nc nodeConfig,
	dataChans []chan<- dataMsg) (error, chan<- *ctrlMsg) {

	listen, err := nc.config.GetString(name, "listen")
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{"name": name}).Error(
			"attribute 'listen' must be specified in this section")
		return err, nil
	}

	//
	// If not set, will default to false, but let's be clear.
	logData, _ := nc.config.GetBool(name, "logdata")
	if err != nil {
		logData = false
	}

	encap, err := nc.config.GetString(name, "encap")
	if err != nil {
		encap = "st"
	}

	keepalive, _ := nc.config.GetInt(name, "keepalive_seconds")

	//
	// Create a control channel which will be used to control us,
	// and kick off the server which will accept connections and
	// listen for control requests.
	ctrlChan := make(chan *ctrlMsg)
	err = addTCPServer(
		name, listen, encap, dataChans, ctrlChan, keepalive, logData)

	return err, ctrlChan
}
