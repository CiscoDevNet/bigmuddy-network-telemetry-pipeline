//
// January 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//

package main

import (
	"encoding/json"
	"fmt"
	"github.com/dlintw/goconf"
	"strings"
	"testing"
	"time"
)

var kmod kafkaOutputModule

type dataMsgKafkaTest struct {
	content []byte
}

func (m *dataMsgKafkaTest) getDataMsgDescription() string {
	return "kafka test msg"
}

func (m *dataMsgKafkaTest) getDataMsgStreamType() dataMsgStreamType {
	// doesn't matter
	return dMStreamGPB
}

func (m *dataMsgKafkaTest) produceByteStream(*dataMsgStreamSpec) (
	error, []byte) {
	return nil, m.content
}

func (m *dataMsgKafkaTest) getMetaDataPath() (error, string) {
	return nil, "RootOperKafkaTest"
}

func (m *dataMsgKafkaTest) getMetaDataIdentifier() (error, string) {
	return nil, "RouterInSpace"
}

func (m *dataMsgKafkaTest) getMetaDataCollectionID() (error, uint64) {
	return nil, 42
}

func (m *dataMsgKafkaTest) getMetaData() *dataMsgMetaData {
	return &dataMsgMetaData{
		Path:       "RootOperKafkaTest",
		Identifier: "RouterInSpace",
	}
}

func (m *dataMsgKafkaTest) produceMetrics(
	spec *metricsSpec,
	outputHandler metricsOutputHandler,
	buf metricsOutputContext) error {
	return fmt.Errorf("GPB CODEC: metric extraction unsupported")
}

func TestKafkaNegative(t *testing.T) {

	var nc nodeConfig

	cfg, err := goconf.ReadConfigFile("pipeline_test_bad.conf")
	nc.config = cfg
	err, _, _ = kmod.configure("mykafka", nc)
	if err == nil {
		t.Error("setup function did not fail on bad broker list")
	}

	cfg, err = goconf.ReadConfigFile("pipeline_test_bad.conf")
	nc.config = cfg
	err, _, _ = kmod.configure("myotherkafka", nc)
	if err == nil {
		t.Error("setup function did not fail on bad key config")
	}
}

func TestKafkaConnect(t *testing.T) {
	var nc nodeConfig

	cfg, err := goconf.ReadConfigFile("pipeline_test.conf")
	nc.config = cfg
	err, _, ctrl := kmod.configure("mykafka", nc)
	if err != nil {
		t.Errorf("setup function fail to connect to kafka. Is kafka up?")
		return
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
	ctrl <- request

	// Wait for ACK
	ack := <-respChan

	if ack.id != ACK {
		t.Error("failed to recieve acknowledgement indicating shutdown complete")
	}

}

func TestKafkaClosedDataFeed(t *testing.T) {
	var nc nodeConfig

	cfg, err := goconf.ReadConfigFile("pipeline_test.conf")
	nc.config = cfg
	err, data, ctrl := kmod.configure("mykafka", nc)
	if err != nil {
		t.Errorf("setup function fail to connect to kafka. Is kafka up?")
		return
	}

	//
	// Test shutdown
	respChan := make(chan *ctrlMsg)
	request := &ctrlMsg{
		id:       SHUTDOWN,
		respChan: respChan,
	}

	// close data connection and wait to make sure select will
	// have the time to mull over it.
	close(data)
	time.Sleep(1)

	//
	// Send shutdown message
	ctrl <- request

	// Wait for ACK
	ack := <-respChan

	if ack.id != ACK {
		t.Error("failed to recieve acknowledgement indicating shutdown complete")
	}

}

func TestKafkaDataFlow(t *testing.T) {

	var msgcontent string
	var stats msgStats
	var msgcount, i uint64
	var nc nodeConfig

	// number of messages to send and how long to give up waiting
	// for book keeping from sending routing to match up
	msgcount = 1000
	giveup := 60

	cfg, err := goconf.ReadConfigFile("pipeline_test.conf")
	nc.config = cfg
	err, data, ctrl := kmod.configure("mykafka", nc)
	if err != nil {
		t.Errorf("setup function fail to connect to kafka. Is kafka up?")
		return
	}

	//
	// Test data...
	for i = 0; i < msgcount; i++ {
		msgcontent = fmt.Sprintf("message number %d", i)
		msg := &dataMsgKafkaTest{
			content: []byte(msgcontent),
		}
		data <- msg
	}

	iterations := 0
	for {
		time.Sleep(1 * time.Second)
		respChan := make(chan *ctrlMsg)
		request := &ctrlMsg{
			id:       REPORT,
			respChan: respChan,
		}
		ctrl <- request
		// Wait for report
		reply := <-respChan

		err = json.Unmarshal(reply.content, &stats)

		if (stats.MsgsOK+stats.MsgsNOK) == msgcount ||
			iterations > giveup {
			//
			// We either got them all, or we've given up
			break
		}
		iterations++

	}

	if err != nil {
		t.Error("failed to recieve msg stats")
	}
	if stats.MsgsOK != msgcount {
		t.Errorf("msgs fed %d, send %d, errored %d",
			msgcount, stats.MsgsOK, stats.MsgsNOK)
	}

	// close data connection and wait to make sure select will
	// have the time to mull over it.
	close(data)
	time.Sleep(1 * time.Second)

	//
	// Send shutdown message
	respChan := make(chan *ctrlMsg)
	request := &ctrlMsg{
		id:       SHUTDOWN,
		respChan: respChan,
	}
	ctrl <- request

	// Wait for ACK
	ack := <-respChan

	if ack.id != ACK {
		t.Error("failed to recieve acknowledgement indicating shutdown complete")
	}

}

var kConsumer kafkaInputModule
var tap tapOutputModule

func TestKafkaConsumer(t *testing.T) {
	var msgcontent string
	var nc nodeConfig
	var ctrl chan<- *ctrlMsg
	var msgcount, i uint64

	msgcount = 20

	dataChan := make(chan dataMsg, 2)
	dataChans := []chan<- dataMsg{dataChan}

	cfg, err := goconf.ReadConfigFile("pipeline_test_bad.conf")
	nc.config = cfg
	badsections := []string{
		"kafkaconsumernoconsumergroup",
		"kafkaconsumerbadkey",
		"kafkaconsumerbadencoding",
		"kafkaconsumernobroker",
		"kafkaBADrequiredAcks",
		"kafkaBADTopicTemplace",
	}
	for _, section := range badsections {
		err, ctrl = kConsumer.configure(section, nc, dataChans)
		if err == nil {
			t.Errorf("kafka consumer section [%v] should fail\n", section)
		}
	}

	cfg, err = goconf.ReadConfigFile("pipeline_test.conf")
	nc.config = cfg

	//
	// Setup tap so we can watch the content collected too
	err, tapDataOut, tapCtrlOut := tap.configure("inspector", nc)
	if err != nil {
		t.Fatalf("setup function failed to setup tap [%v]", err)
	}
	dataChans = append(dataChans, tapDataOut)

	//
	// Setup consumer
	err, ctrl = kConsumer.configure("kafkaconsumer", nc, dataChans)
	if err != nil {
		t.Fatalf("setup function failed to setup consumer")
	}

	//
	// Setup to publish messages, and make sure we receive them.
	err, dataOut, ctrlOut := kmod.configure("mykafka2", nc)
	if err != nil {
		t.Fatalf("setup function fail to setup producer. Is kafka up?")
	}

	//
	// Iterate until connection is confirmed...
outer:
	for {
		dataOut <- &dataMsgKafkaTest{
			content: []byte(`["Testing testing testing, 1, 2, 3]`),
		}
		select {
		case <-dataChan:
			break outer
		case <-time.After(1 * time.Second):
			continue
		}
	}

	// t.Logf("Producing %v message\n", msgcount)
	for i = 0; i < msgcount; i++ {
		msgcontent = fmt.Sprintf(`{"list":[` + strings.Repeat(`"ABC",`, 3000) + `"DEF"]}`)
		msg := &dataMsgKafkaTest{
			content: []byte(msgcontent),
		}
		dataOut <- msg
	}

	//
	// Read all the messages.
	// t.Logf("Consuming %v message\n", msgcount)
	for i = 0; i < msgcount; i++ {
		<-dataChan
	}

	time.Sleep(1 * time.Second)

	//t.Logf("Producing %v message\n", msgcount)
	for i = 0; i < msgcount; i++ {
		msgcontent = fmt.Sprintf(`{"list":[` + strings.Repeat(`"ABC",`, 3000) + `"DEF"]}`)
		msg := &dataMsgKafkaTest{
			content: []byte(msgcontent),
		}
		dataOut <- msg
	}

	//
	// Read all the messages.
	//t.Logf("Consuming %v message\n", msgcount)
	for i = 0; i < msgcount; i++ {
		<-dataChan
	}

	time.Sleep(2 * time.Second)

	respChan := make(chan *ctrlMsg)
	request := &ctrlMsg{
		id:       SHUTDOWN,
		respChan: respChan,
	}

	ctrlOut <- request
	// Wait for ACK
	ack := <-respChan
	if ack.id != ACK {
		t.Error("failed to recieve ack for producer shutdown complete")
	}

	respChan = make(chan *ctrlMsg)
	request.respChan = respChan
	ctrl <- request
	ack = <-respChan
	if ack.id != ACK {
		t.Error("failed to recieve ack for consumer shutdown complete")
	}

	respChan = make(chan *ctrlMsg)
	request.respChan = respChan
	tapCtrlOut <- request
	ack = <-respChan
	if ack.id != ACK {
		t.Error("failed to recieve ack for tap shutdown complete")
	}

}

func TestKafkaDynamicTopic(t *testing.T) {
	var msgcontent string
	var nc nodeConfig
	var msgcount, i uint64

	msgcount = 2

	cfg, err := goconf.ReadConfigFile("pipeline_test.conf")
	if err != nil {
		t.Errorf("kafka read pipeline test config fail [%v]\n", err)
	}
	nc.config = cfg
	pairs := [][]string{
		{"kafkaAout", "kafkaAin"},
		{"kafkaBout", "kafkaBin"},
		{"kafkaCout", "kafkaCin"},
		{"kafkaDout", "kafkaDin"},
	}

	for _, section := range pairs {
		dataChan := make(chan dataMsg, 2)
		dataChans := []chan<- dataMsg{dataChan}

		//fmt.Printf("Configured [%v]\n", section[1])
		err, ctrlIn := kConsumer.configure(section[1], nc, dataChans)
		if err != nil {
			t.Errorf("kafka consumer section [%v] fail [%v]\n", section[1], err)
		}

		//fmt.Printf("Configured [%v]\n", section[0])
		err, dataOut, ctrlOut := kmod.configure(section[0], nc)
		if err != nil {
			t.Fatalf("setup function fail to setup producer. Is kafka up [%v]?", err)
		}

		//
		// Iterate until connection is confirmed...
	outer:
		for {
			dataOut <- &dataMsgKafkaTest{
				content: []byte(`["Testing testing testing, 1, 2, 3]`),
			}
			select {
			case <-dataChan:
				break outer
			case <-time.After(1 * time.Second):
				continue
			}
		}

		//fmt.Printf("Producing %v message %v\n", msgcount, section[0])
		for i = 0; i < msgcount; i++ {
			msgcontent = fmt.Sprintf(`{"list":[` + strings.Repeat(`"ABC",`, 10) + `"DEF"]}`)
			msg := &dataMsgKafkaTest{
				content: []byte(msgcontent),
			}
			dataOut <- msg
		}

		//
		// Read all the messages.
		//fmt.Printf("Consuming %v message %v\n", msgcount, section[1])
		for i = 0; i < msgcount; i++ {
			<-dataChan
		}

		time.Sleep(2 * time.Second)

		respChan := make(chan *ctrlMsg)
		request := &ctrlMsg{
			id:       SHUTDOWN,
			respChan: respChan,
		}

		ctrlOut <- request
		ack := <-respChan
		if ack.id != ACK {
			t.Error("failed to receive ack for producer shutdown complete")
		}

		ctrlIn <- request
		ack = <-respChan
		if ack.id != ACK {
			t.Error("failed to receive ack for consumer shutdown complete")
		}

	}

}
