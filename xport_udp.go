//
// October 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Handle UDP transports

package main

import (
	"encoding/hex"
	"encoding/json"
	_ "fmt"
	log "github.com/sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"net"
	"time"
)

const (
	XPORT_UDPPOLLWAIT = 3
	XPORT_UDPRETRY    = 10
)

type serverUDP struct {
	name      string
	bindpoint *net.UDPAddr
	encap     string
	// OS receive buffer, can be tuned in config.
	rxBuf int
	//
	// Log data into debug log
	logData bool
	// Listener over which connections are accepted.
	listener *net.UDPConn
	// Control channel used to control the server
	ctrlChan <-chan *ctrlMsg
	// Data channels fed by the server
	dataChans []chan<- dataMsg
	//
	// Channel used by listen handler to signal it has closed.
	closedListener chan struct{}
	//
	// Cancelled by conductor?
	cancelled chan struct{}
}

//
// runServer is a UDP handler.
func (s *serverUDP) runServer() {

	conn := s.listener
	defer close(s.closedListener)

	logctx := logger.WithFields(log.Fields{
		"name":  s.name,
		"local": conn.LocalAddr().String()})
	logctx.Info("UDP server run starting")

	if s.rxBuf != 0 {
		err := conn.SetReadBuffer(s.rxBuf)
		if err != nil {
			logctx.WithError(err).WithFields(
				log.Fields{
					"rxBuf": s.rxBuf,
				}).Error(
				"RxBuf size (check OS max, e.g. sysctl -w net.core.rmem_max)")
		}
	}

	//
	// Fetch a parser
	err, parser := getNewEncapParser(s.name, s.encap, nil)
	if err != nil {
		logctx.WithError(err).Error("UDP parser setup")
		return
	}

	for {
		err, buffer := parser.nextBlockBuffer()
		if err != nil {
			logctx.WithError(err).Error("UDP failed to retrieve buffer")
			goto out
		}

		length, remoteAddr, err := conn.ReadFromUDP(*buffer)
		if err != nil {
			//
			// This may be normal operation; i.e. parent closed
			// binding. We have no way of distinguishing short of
			// some horrid match of the error string.
			// https://github.com/golang/go/issues/4373
			//
			// But we can check for cancelled...
			select {
			case <-s.cancelled:
				logctx.WithFields(
					log.Fields{
						"remote": remoteAddr.String(),
					}).Debug("Reading from UDP port, cancelled")
			default:
				xportUDPMetaMonitor.CountersErrors.WithLabelValues(
					s.name, remoteAddr.String()).Inc()
				logctx.WithError(err).WithFields(
					log.Fields{
						"remote": remoteAddr.String(),
					}).Error("Reading from UDP port")
			}
			goto out
		}

		trimBuf := (*buffer)[:length]
		// fmt.Printf("length: %d/%d\n", length, len(trimBuf))
		err, msgs := parser.nextBlock(trimBuf, remoteAddr)
		if err != nil {
			xportUDPMetaMonitor.CountersErrors.WithLabelValues(
				s.name, remoteAddr.String()).Inc()
			logctx.WithError(err).WithFields(
				log.Fields{
					"remote": remoteAddr.String(),
				}).Error("Failed to extract next buffer")
			goto out
		}

		if s.logData {
			logctx.WithFields(log.Fields{
				"remote":       remoteAddr.String(),
				"dataMsgCount": len(msgs),
				"msglen":       len(trimBuf),
				"msg":          hex.EncodeToString(trimBuf),
			}).Debug("UDP server logdata")
		}
		xportUDPMetaMonitor.CountersMsgs.WithLabelValues(
			s.name, remoteAddr.String()).Inc()
		xportUDPMetaMonitor.CountersBytes.WithLabelValues(
			s.name, remoteAddr.String()).Add(float64(length))

		if msgs == nil {
			continue
		}

		//
		// Now we have content. What to do with it?
		//
		// Spray the generated messages across each available
		// downstream channel
		//
		// Given this is UDP, rather than block on channel if channel
		// is full, we drop and count. This ensures that the drop
		// damage is limited to the slow consumer rather than all
		// consumers. (There is still a window of opportunity between
		// capacity test and send, if other producers feed the channel
		// but we can live with that.)
		//
		for _, msg := range msgs {
			for _, dataChan := range s.dataChans {
				if cap(dataChan) == len(dataChan) {
					// Count drops and continue. We need to add metadata
					// to channel to do a better job of identifying
					// laggards.
					xportUDPMetaMonitor.CountersDrops.WithLabelValues(
						s.name, remoteAddr.String()).Inc()
					continue
				}
				select {
				case dataChan <- msg:
					// job done for this msg on this channel
				case <-s.cancelled:
					goto out
				}
			}
		}
	}

out:
	logctx.Info("UDP server run stopping")
}

func (s *serverUDP) startStickyServer() {
	//
	// Prime loop by closing listener channel
	close(s.closedListener)

	for {
		select {
		case <-s.closedListener:
			//
			// Listener is closed. Recreate listener, set up new
			// closedListener and kick off.
			var err error
			s.listener, err = net.ListenUDP("udp", s.bindpoint)
			if err != nil {
				logger.WithError(err).WithFields(log.Fields{
					"name":      s.name,
					"bindpoint": s.bindpoint,
				}).Error("UDP server failed to bind, retrying")
				time.Sleep(time.Second * XPORT_UDPRETRY)
				continue
			} else {
				s.closedListener = make(chan struct{})
				go s.runServer()
			}

		case msg := <-s.ctrlChan:
			switch msg.id {
			case REPORT:
				stats := msgStats{}
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
					"UDP server loop, rxed SHUTDOWN, closing binding")

				close(s.cancelled)
				if s.listener != nil {
					s.listener.Close()
					//
					// Closing the listen port will cause reading from
					// it to fail, and running server to return.
					// Wait for signal we're done
					<-s.closedListener
				}

				logger.WithFields(
					log.Fields{
						"name":      s.name,
						"bindpoint": s.bindpoint,
					}).Debug("UDP server notify conductor binding is closed")
				resp := &ctrlMsg{
					id:       ACK,
					respChan: nil,
				}
				msg.respChan <- resp
				return

			default:
				logger.WithFields(
					log.Fields{"name": s.name}).Error(
					"UDP server loop, unknown ctrl message")
			}
		}
	}
}

func addUDPServer(
	name string,
	bindpoint *net.UDPAddr,
	encap string,
	dataChans []chan<- dataMsg,
	ctrlChan <-chan *ctrlMsg,
	rxBuf int,
	logData bool) error {

	s := new(serverUDP)
	s.name = name
	s.bindpoint = bindpoint
	s.encap = encap
	s.logData = logData
	s.dataChans = dataChans
	s.ctrlChan = ctrlChan
	s.rxBuf = rxBuf
	s.closedListener = make(chan struct{})
	s.cancelled = make(chan struct{})

	go s.startStickyServer()

	return nil
}

// Module implement inputNodeModule interface
type udpInputModule struct {
}

func udpInputModuleNew() inputNodeModule {
	return &udpInputModule{}
}

func (m *udpInputModule) configure(
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

	bindpoint, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{"name": name}).Error(
			"attribute 'listen' unparseable as local UDP address")
		return err, nil
	}

	encap, err := nc.config.GetString(name, "encap")
	if err != nil {
		encap = "st"
	}
	//
	// If not set, will default to false, but let's be clear.
	logData, _ := nc.config.GetBool(name, "logdata")
	rxBuf, _ := nc.config.GetInt(name, "rxbuf")

	//
	// Create a control channel which will be used to control us,
	// and kick off the server which will accept connections and
	// listen for control requests.
	ctrlChan := make(chan *ctrlMsg)
	err = addUDPServer(
		name, bindpoint, encap, dataChans, ctrlChan, rxBuf, logData)

	return err, ctrlChan
}

type xportUDPMetaMonitorType struct {
	CountersMsgs   *prometheus.CounterVec
	CountersBytes  *prometheus.CounterVec
	CountersErrors *prometheus.CounterVec
	CountersDrops  *prometheus.CounterVec
}

var xportUDPMetaMonitor *xportUDPMetaMonitorType

func init() {
	xportUDPMetaMonitor = &xportUDPMetaMonitorType{
		CountersMsgs: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "xportUDP_messages",
				Help: "Messages",
			},
			[]string{"section", "peer"}),
		CountersBytes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "xportUDP_bytes",
				Help: "Bytes",
			},
			[]string{"section", "peer"}),
		CountersErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "xportUDP_errors",
				Help: "Errors",
			},
			[]string{"section", "peer"}),
		CountersDrops: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "xportUDP_drops",
				Help: "Drops",
			},
			[]string{"section", "peer"}),
	}

	// Dump content
	prometheus.MustRegister(xportUDPMetaMonitor.CountersMsgs)
	prometheus.MustRegister(xportUDPMetaMonitor.CountersBytes)
	prometheus.MustRegister(xportUDPMetaMonitor.CountersErrors)
	prometheus.MustRegister(xportUDPMetaMonitor.CountersDrops)
}
