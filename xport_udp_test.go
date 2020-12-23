package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	log "github.com/sirupsen/logrus"
	samples "github.com/cisco/bigmuddy-network-telemetry-pipeline/mdt_msg_samples"
	"github.com/dlintw/goconf"
	"net"
	"sync"
	"testing"
	"time"
)

// Workaround for uninit lock assignment go vet error:
// https://github.com/golang/go/issues/13675

type udpTestContextLock struct {
	sync.Mutex
	sync.WaitGroup
}

type udpTestContext struct {
	name     string
	maxlag   int
	max      int
	encap    encapSTHdrMsgEncap
	send     int
	handled  int
	dataChan chan dataMsg
	tDone    chan struct{}
	lock     *udpTestContextLock
	conn     *net.UDPConn
}

func udpTestSendOneMessage(
	sample *samples.SampleTelemetryTableEntry,
	context samples.MDTContext) (abort bool) {

	var err error

	c := context.(*udpTestContext)

	hdr := encapSTHdr{
		MsgType:       ENC_ST_HDR_MSG_TYPE_TELEMETRY_DATA,
		MsgEncap:      ENC_ST_HDR_MSG_ENCAP_GPB,
		MsgHdrVersion: ENC_ST_HDR_VERSION,
		Msgflag:       ENC_ST_HDR_MSG_FLAGS_NONE,
	}

	hdr.MsgEncap = c.encap

	if c.encap == ENC_ST_HDR_MSG_ENCAP_GPB {
		hdr.Msglen = uint32(len(sample.SampleStreamGPB))
	} else if c.encap == ENC_ST_HDR_MSG_ENCAP_JSON {
		hdr.Msglen = uint32(len(sample.SampleStreamJSON))
	} else {
		return true
	}

	hdrBuf := new(bytes.Buffer)
	err = binary.Write(hdrBuf, binary.BigEndian, hdr)
	if err != nil {
		panic(err)
	}
	if c.encap == ENC_ST_HDR_MSG_ENCAP_GPB {
		_, err = c.conn.Write(append(hdrBuf.Bytes(), sample.SampleStreamGPB...))
	} else if c.encap == ENC_ST_HDR_MSG_ENCAP_JSON {
		_, err = c.conn.Write(append(hdrBuf.Bytes(), sample.SampleStreamJSON...))
	}
	if err == nil {
		c.lock.Lock()
		c.send++
		c.lock.Unlock()
	}

	return false
}

func udpTestSendMessages(c *udpTestContext) {
	defer c.lock.Done()
	for {
		select {
		case <-c.tDone:
			return
		default:
			takeabreak := false
			done := false
			c.lock.Lock()
			if c.handled >= c.max {
				done = true
			}
			if c.send-c.handled >= c.maxlag {
				takeabreak = true
			}
			c.lock.Unlock()

			if done {
				return
			}

			if takeabreak {
				// fmt.Printf("send: waiting 1s at t %d r %d\n", c.send, c.handled)
				time.Sleep(time.Millisecond * 2)
				continue
			}

			samples.MDTSampleTelemetryTableIterate(
				samples.SAMPLE_TELEMETRY_DATABASE_BASIC,
				udpTestSendOneMessage, c)
		}
	}
}

func udpTestHandleMessages(c *udpTestContext) {
	for {
		select {
		case <-c.dataChan:
			c.lock.Lock()
			c.handled++
			c.lock.Unlock()
		case <-c.tDone:
			// fmt.Printf("receive: stop at t %d r %d\n", c.send, c.handled)
			c.lock.Done()
			return
		}
	}
}

func TestUDPServerNegative(tb *testing.T) {
	var dataChans = make([]chan<- dataMsg, 0)
	var nc nodeConfig
	var err error

	logfile := startup()
	logger = theLogger.WithFields(log.Fields{"tag": "test"})
	if logfile != nil {
		defer logfile.Close()
	}

	nc.config, err = goconf.ReadConfigFile("pipeline_test.conf")
	dataChan := make(chan dataMsg, DATACHANNELDEPTH)
	dataChans = append(dataChans, dataChan)

	i := udpInputModuleNew()
	err, _ = i.configure("udpinnolisten", nc, dataChans)
	if err == nil {
		tb.Fatal("Configured input module without listen")
	}

	err, _ = i.configure("udpinbadlisten", nc, dataChans)
	if err == nil {
		tb.Fatal("Configured input module with bad listen")
	}

}

func TestUDPServer(tb *testing.T) {
	var dataChans = make([]chan<- dataMsg, 0)
	var nc nodeConfig
	var err error

	logfile := startup()
	logger = theLogger.WithFields(log.Fields{"tag": "test"})
	if logfile != nil {
		defer logfile.Close()
	}

	nc.config, err = goconf.ReadConfigFile("pipeline_test.conf")
	go metamonitoring_init(nc)

	server, err := nc.config.GetString("udpin", "listen")
	if err != nil {
		tb.Fatal("Failed to pick up 'listen'", err)
	}

	dataChan := make(chan dataMsg, DATACHANNELDEPTH)
	dataChans = append(dataChans, dataChan)

	i := udpInputModuleNew()
	err, ctrlChan := i.configure("udpin", nc, dataChans)
	if err != nil {
		tb.Fatal("Failed to config input module", err)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		tb.Fatal("Failed to get server address", err)
	}
	outConn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		tb.Fatal("Failed to dial UDP", err)
	}

	//
	// Read, as opposed to write buffer is the limitation for bursts.
	// No need to setup outConn.SetWriteBuffer(46388608)
	//
	subtests := []udpTestContext{
		{name: "BurstSparseGPB", maxlag: 5, max: 10000, encap: ENC_ST_HDR_MSG_ENCAP_GPB},
		{name: "BurstMediumGPB", maxlag: 50, max: 10000, encap: ENC_ST_HDR_MSG_ENCAP_GPB},
		{name: "BurstDenseGPB", maxlag: 500, max: 10000, encap: ENC_ST_HDR_MSG_ENCAP_GPB},
	}

	//
	// Structure test as subtest in case I need to add more.
	for _, subt := range subtests {
		subt.lock = &udpTestContextLock{}
		tb.Run(
			fmt.Sprint(subt.name),
			func(tb *testing.T) {
				subt.tDone = make(chan struct{})
				subt.conn = outConn
				subt.dataChan = dataChan

				subt.lock.Add(2)

				go udpTestHandleMessages(&subt)
				time.Sleep(time.Millisecond * 500)
				go udpTestSendMessages(&subt)

				ticker := time.NewTicker(time.Second * 2)
				old_handled := 0
				for _ = range ticker.C {
					subt.lock.Lock()
					if subt.handled >= subt.max {
						tb.Logf("Complete: handled %d, sent %d, max %d",
							subt.handled, subt.send, subt.max)

						ticker.Stop()
						break
					}

					if subt.handled == old_handled {
						//
						// stopped making progress
						ticker.Stop()
						tb.Fatalf("Progress stalled: handled %d, sent %d, max %d",
							subt.handled, subt.send, subt.max)
						break
					}
					old_handled = subt.handled
					subt.lock.Unlock()
				}

				close(subt.tDone)
				subt.lock.Wait()
			})
	}

	//
	// Test shutdown
	respChan := make(chan *ctrlMsg)
	request := &ctrlMsg{
		id:       SHUTDOWN,
		respChan: respChan,
	}

	//
	// Send shutdown message
	ctrlChan <- request

	// Wait for ACK
	ack := <-respChan

	if ack.id != ACK {
		tb.Error("failed to recieve acknowledgement indicating shutdown complete")
	}

}
