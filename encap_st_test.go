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
	samples "github.com/cisco/bigmuddy-network-telemetry-pipeline/mdt_msg_samples"
	"testing"
)

type testSource struct{}

func (t *testSource) String() string {
	return "TEST source"
}

var encapSTTestSource testSource

func TestSTParser(t *testing.T) {

	err, gp := getNewEncapSTParser("ENCAP ST TEST", &encapSTTestSource)
	if err != nil {
		t.Errorf("Failed to get ST test parser: %v", err)
	}

	// Type assert gp to the more specific so we can manipulate ST
	// specific state.
	p := gp.(*encapSTParser)

	err, _ = p.nextBlockBuffer()
	if err != nil {
		t.Errorf("Failed to get header buffer: %v", err)
	}

	//
	// Cheat to force error cases
	p.state = ENC_ST_WAIT_FOR_DATA
	err, _ = p.nextBlockBuffer()
	if err == nil {
		t.Errorf("Should have failed with request to get 0 size buffer")
	}

	p.nextBlockSize = ENC_ST_MAX_PAYLOAD + 1
	err, _ = p.nextBlockBuffer()
	if err == nil {
		t.Errorf("Should have failed with request to get oversize buffer")
	}

	//
	// Get a GPB message and encap it in ST header.
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

	encodedMsgForLen := make([]byte, 0, len(payload)+256)
	hdrbuf := bytes.NewBuffer(encodedMsgForLen)
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

	encodedMsg := hdrbuf.Bytes()
	// Star afresh
	err, gp = getNewEncapSTParser("ENCAP ST TEST", &encapSTTestSource)
	if err != nil {
		t.Errorf("Failed to get fresh ST test parser: %v", err)
	}
	p = gp.(*encapSTParser)

	err, dMs := p.nextBlock(encodedMsg, nil)
	if err != nil {
		t.Errorf("Failed to parse header: %v", err)
	}
	if dMs != nil {
		t.Errorf("Expected header but got data.")
	}
	if p.state != ENC_ST_WAIT_FOR_DATA {
		t.Errorf("Failed to transition to wait for payload")
	}
	if p.nextBlockSize != uint32(len(payload)) {
		t.Errorf("Failed to parser and track msg len")
	}

	err, dataBuf := p.nextBlockBuffer()
	if err != nil {
		t.Errorf("Failed to get data buffer")
	}

	if len(*dataBuf) != len(payload) {
		t.Errorf("Databuf returned len %d expected %d",
			len(*dataBuf), 16)
	}

	err, dMs = p.nextBlock(payload, nil)
	if err != nil {
		t.Errorf("Failed to get data message")
	}

	if dMs == nil || len(dMs) != 1 {
		t.Errorf("Failed to extract expected data")
	}

	err, _ = dMs[0].produceByteStream(dataMsgStreamSpecDefault)
	if err != nil {
		t.Errorf("Failed to produce byte stream from dataMsg")
	}
}
