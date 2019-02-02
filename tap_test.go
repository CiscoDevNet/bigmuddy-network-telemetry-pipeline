//
// November 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//

package main

import (
	samples "github.com/nleiva/pipeline/mdt_msg_samples"
	"github.com/dlintw/goconf"
	"testing"
	"time"
)

type tapTestCtrl struct {
}

func (t *tapTestCtrl) String() string {
	return "TapTest"
}

//
// TestTAPBinaryWriteRead reads a sample message, generates the binary
// dump file, and rereads that file using tap in
func TestTAPBinaryWriteRead(t *testing.T) {
	var nc nodeConfig
	var tapTestCtrlSrc tapTestCtrl

	logger.Info("Start TestTAPBinaryWriteRead")
	sample := samples.MDTSampleTelemetryTableFetchOne(
		samples.SAMPLE_TELEMETRY_DATABASE_BASIC)
	if sample == nil {
		t.Errorf("Failed to fetch data")
		return
	}

	err, codec := getNewCodecGPB("test", ENCODING_GPB)
	if err != nil {
		t.Error("Failed to get a GPB codec to test")
	}

	err, msgs := codec.blockToDataMsgs(&tapTestCtrlSrc, sample.SampleStreamGPB)
	if err != nil {
		t.Error("Failed to get a GPB codec to test")
	}

	logger.Info("Read config")
	cfg, err := goconf.ReadConfigFile("pipeline_test_tap.conf")
	if err != nil {
		t.Fatal("read config failed")
	}

	section := "tap_out_bin"
	nc.config = cfg

	out := tapOutputModuleNew()
	err, inject, ocmc := out.configure(section, nc)
	if err != nil {
		t.Errorf("tap section [%v] failed\n", section)
	}

	logger.Info("Injecting content")
	for i := 0; i < 10; i++ {
		inject <- msgs[0]
		//
		// Now that messages are injected, we should be able to pick
		// them up again.
	}

	in := replayInputModuleNew()
	section = "replay_bin"
	dataChan := make(chan dataMsg, 1000)
	dataChans := []chan<- dataMsg{dataChan}
	err, icmc := in.configure(section, nc, dataChans)
	if err != nil {
		t.Errorf("tap section [%v] failed\n", section)
	}

	logger.Info("Now let's read it")
	for i := 0; i < 10; i++ {
		dM := <-dataChan
		t.Log(dM.getDataMsgDescription())
	}

	respChan := make(chan *ctrlMsg)
	request := &ctrlMsg{
		id:       SHUTDOWN,
		respChan: respChan,
	}

	logger.Info("Shutdown reader")
	icmc <- request
	// Wait for ACK
	ack := <-respChan
	if ack.id != ACK {
		t.Error("failed to recieve ack for tap in shutdown complete")
	}

	logger.Info("Shutdown writer")
	ocmc <- request
	// Wait for ACK
	ack = <-respChan
	if ack.id != ACK {
		t.Error("failed to recieve ack for tap out shutdown complete")
	}
}

//
// TestTAPBinaryReadWrite taps in from a binary dump, and produces a
// hex dump
func TestTAPBinaryReadWrite(t *testing.T) {
	var nc nodeConfig

	cfg, err := goconf.ReadConfigFile("pipeline_test.conf")
	if err != nil {
		t.Error("read config failed")
	}

	nc.config = cfg
	out := tapOutputModuleNew()

	section := "tap_out_bin_hexdump"
	err, inject, ocmc := out.configure(section, nc)
	if err != nil {
		t.Errorf("tap section [%v] failed\n", section)
	}

	in := replayInputModuleNew()
	section = "replay_bin_archive"
	dataChan := make(chan dataMsg, 1000)
	dataChans := []chan<- dataMsg{dataChan}
	err, icmc := in.configure(section, nc, dataChans)
	if err != nil {
		t.Errorf("tap section [%v] failed\n", section)
	}
	//
	// 7 message in replay_bin_archive
	for i := 0; i < 7; i++ {
		dM := <-dataChan
		t.Log(dM.getDataMsgDescription())
		inject <- dM
	}

	//
	// Check handling of closing of data chan before shutdown giving
	// handler opportunity to exercise before actually shutting down.
	close(inject)
	time.Sleep(100 * time.Millisecond)

	respChan := make(chan *ctrlMsg)
	request := &ctrlMsg{
		id:       SHUTDOWN,
		respChan: respChan,
	}

	icmc <- request
	// Wait for ACK
	ack := <-respChan
	if ack.id != ACK {
		t.Error("failed to recieve ack for tap in shutdown complete")
	}

	ocmc <- request
	// Wait for ACK
	ack = <-respChan
	if ack.id != ACK {
		t.Error("failed to recieve ack for tap out shutdown complete")
	}

}
