//
// June 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//

package main

import (
	"github.com/dlintw/goconf"
	"testing"
	"time"
)

func TestMetricsConfigureNegative(t *testing.T) {
	var nc nodeConfig

	mod := metricsOutputModuleNew()

	cfg, err := goconf.ReadConfigFile("pipeline_test_bad.conf")
	nc.config = cfg

	badsections := []string{
		"metricsbad_missingfilename",
		"metricsbad_missingfile",
		"metricsbad_badjson",
		"metricsbad_missingoutput",
		"metricsbad_unsupportedoutput",
		"metricsbad_missingpushgw",
	}

	for _, section := range badsections {
		err, _, _ = mod.configure(section, nc)
		if err == nil {
			t.Errorf("metrics section section [%v] should fail\n", section)
		}
	}
}

func TestMetricsConfigure(t *testing.T) {
	var nc nodeConfig
	var codecJSONTestSource testSource

	mod := metricsOutputModuleNew()

	cfg, err := goconf.ReadConfigFile("pipeline_test.conf")
	nc.config = cfg
	err, dChan, cChan := mod.configure("mymetrics", nc)

	err, p := getNewCodecJSON("JSON CODEC TEST")
	if err != nil {
		t.Errorf("Failed to get JSON codec [%v]", err)
		return
	}

	testJSONMsg := []byte(`{"Name":"Alice","Body":"Hello","Test":1294706395881547000}`)
	err, dMs := p.blockToDataMsgs(&codecJSONTestSource, testJSONMsg)

	if err != nil {
		t.Errorf("Failed to get messages from JSON stream [%v]", err)
		return
	}

	dM := dMs[0]

	dChan <- dM

	time.Sleep(1 * time.Second)

	//
	// Send shutdown message
	respChan := make(chan *ctrlMsg)
	request := &ctrlMsg{
		id:       SHUTDOWN,
		respChan: respChan,
	}
	cChan <- request

	// Wait for ACK
	ack := <-respChan

	if ack.id != ACK {
		t.Error("failed to recieve acknowledgement indicating shutdown complete")
	}

}
