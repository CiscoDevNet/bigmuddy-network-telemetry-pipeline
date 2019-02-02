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
	samples "github.com/nleiva/pipeline/mdt_msg_samples"
	"net"
	"testing"
	"time"
)

func TestTCPServerStart(t *testing.T) {
	var dataChans = make([]chan<- dataMsg, 0)

	ctrlChan := make(chan *ctrlMsg)
	ctrlChan1 := make(chan *ctrlMsg)
	ctrlChan2 := make(chan *ctrlMsg)
	dataChan := make(chan dataMsg, DATACHANNELDEPTH)
	dataChans = append(dataChans, dataChan)

	err := addTCPServer("TestTCP", ":5556", "st", dataChans, ctrlChan, 0, true)
	if err != nil {
		t.Errorf("setup function fail to startup TCP server")
		return
	}

	err = addTCPServer("TestTCP", ":5556", "st", dataChans, ctrlChan1, 1000, true)
	//
	// NEGATIVE: This should fail because we are already bound to the same port
	if err == nil {
		t.Errorf("setup function succeded to startup TCP server, but expected fail")
		return
	}

	err = addTCPServer("TestTCP2", ":5559", "st", dataChans, ctrlChan2, 1000, true)
	if err != nil {
		t.Errorf("setup function fail to startup second TCP server (port REALLY in use?)")
		return
	}

	time.Sleep(1 * time.Second)

	//
	// Bring up connection and test sending content
	conn, err := net.Dial("tcp", ":5556")
	if err != nil {
		t.Errorf("Failed to connect to server")
		return
	}

	sample := samples.MDTSampleTelemetryTableFetchOne(
		samples.SAMPLE_TELEMETRY_DATABASE_BASIC)
	if sample == nil {
		t.Errorf("Failed to fetch data")
		return
	}
	fullmsg := sample.SampleStreamGPB
	hdr := encapSTHdr{
		MsgType:       ENC_ST_HDR_MSG_TYPE_TELEMETRY_DATA,
		MsgEncap:      ENC_ST_HDR_MSG_ENCAP_GPB,
		MsgHdrVersion: ENC_ST_HDR_VERSION,
		Msgflag:       ENC_ST_HDR_MSG_FLAGS_NONE,
		Msglen:        uint32(len(fullmsg)),
	}

	err = binary.Write(conn, binary.BigEndian, &hdr)
	if err != nil {
		t.Errorf("Failed to write data header")
		return
	}

	wrote, err := conn.Write(fullmsg)
	if err != nil {
		t.Errorf("Failed write data 1")
		return
	}
	if wrote != len(fullmsg) {
		t.Errorf("Wrote %d, expect %d for data 1",
			wrote, len(fullmsg))
		return
	}

	data := <-dataChan

	err, b := data.produceByteStream(dataMsgStreamSpecDefault)
	if err != nil {
		t.Errorf("Data failed to produce byte stream as expected")
	}

	if !bytes.Contains(b, fullmsg) {
		t.Errorf("Failed to receive expected data")
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
		t.Error("failed to recieve acknowledgement indicating shutdown complete")
	}

	//
	// Send shutdown message
	ctrlChan2 <- request

	// Wait for ACK
	ack = <-respChan

	if ack.id != ACK {
		t.Error("failed to recieve acknowledgement indicating shutdown complete")
	}

}
