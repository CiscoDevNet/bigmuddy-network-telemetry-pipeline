//
// February 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//

package main

import (
	"bytes"
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	samples "github.com/cisco/bigmuddy-network-telemetry-pipeline/mdt_msg_samples"
	"net"
	"testing"
	"time"
)

func TestConductor(t *testing.T) {

	startup()

	logger.Info("Load config, logging", conductor.Logfile)
	conductor.Configfile = "pipeline_test.conf"
	err := loadConfig()
	if err != nil {
		t.Errorf("Config load failed: %v\n", err)
	} else {
		go run()
	}

	logger.Info("Connect TCP :5556")
	conn1, err := net.Dial("tcp", ":5556")
	if err != nil {
		t.Errorf("Failed to connect to server 5556")
		return
	}

	logger.Info("Connect TCP :5557")
	conn2, err := net.Dial("tcp", ":5557")
	if err != nil {
		t.Errorf("Failed to connect to server 5557")
		return
	}

	//
	// Get a GPB message and encap it in ST header.
	logger.Info("Loading MDT samples")
	sample := samples.MDTSampleTelemetryTableFetchOne(
		samples.SAMPLE_TELEMETRY_DATABASE_BASIC)
	if sample == nil {
		t.Errorf("Failed to fetch data")
		return
	}
	payload := sample.SampleStreamGPB
	encap := encapSTHdr{
		MsgType:       ENC_ST_HDR_MSG_TYPE_TELEMETRY_DATA,
		MsgEncap:      ENC_ST_HDR_MSG_ENCAP_GPB,
		MsgHdrVersion: ENC_ST_HDR_VERSION,
		Msgflag:       ENC_ST_HDR_MSG_FLAGS_NONE,
		Msglen:        uint32(len(payload)),
	}

	encodedMsg := make([]byte, 0, len(payload)+256)
	hdrbuf := bytes.NewBuffer(encodedMsg)
	err = binary.Write(hdrbuf, binary.BigEndian, &encap)
	if err != nil {
		t.Errorf("Failed to write data header")
		return
	}
	_, err = hdrbuf.Write(payload)
	if err != nil {
		t.Errorf("Failed write data 1")
		return
	}

	logger.Info("Write to first connection")
	written, err := conn1.Write(encodedMsg)
	if err != nil {
		t.Errorf("Failed write data 1")
		return
	}
	if written != len(encodedMsg) {
		t.Errorf("Wrote %d, expect %d for data 1",
			written, len(encodedMsg))
		return
	}

	logger.Info("Write to second connection")
	written, err = conn2.Write(encodedMsg)
	if err != nil {
		t.Errorf("Failed write data 2")
		return
	}
	if written != len(encodedMsg) {
		t.Errorf("Wrote %d, expect %d for data 2",
			written, len(encodedMsg))
		return
	}

	time.Sleep(5 * time.Second)

	//
	// Diddle outputs and inputs to shut them down.
	logger.Info("Close inputs")
	for _, node := range conductor.inputNodes {
		respChan := make(chan *ctrlMsg)
		request := &ctrlMsg{
			id:       SHUTDOWN,
			respChan: respChan,
		}

		//
		// Send shutdown message
		logger.Info("Closing input", node)
		node.ctrlChan <- request
		// Wait for ACK
		<-respChan
		close(node.ctrlChan)
		logger.Info("    Closed input", node)
	}
	logger.Info("Close outputs")
	for _, node := range conductor.outputNodes {
		respChan := make(chan *ctrlMsg)
		request := &ctrlMsg{
			id:       SHUTDOWN,
			respChan: respChan,
		}

		//
		// Send shutdown message
		logger.Info("Closing output", node)
		node.ctrlChan <- request
		// Wait for ACK
		<-respChan
		close(node.ctrlChan)
		logger.Info("    Closed output", node)
	}

}

//
// Set up logger for tests.
func init() {
	startup()
	// theLogger.Formatter = new(log.JSONFormatter)
	logger = theLogger.WithFields(log.Fields{"tag": "TESTING"})
}
