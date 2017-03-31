//
// February 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Control and data message interfaces and common types.
//
package main

import (
	"fmt"
	"io/ioutil"
	"text/template"
)

type dataMsgStreamType int

const (
	dMStreamGPB dataMsgStreamType = iota
	dMStreamGPBKV
	dMStreamJSON
	dMStreamJSONEvents
	dMStreamTemplate
	dMStreamMsgDefault
	dMStreamMsgUnknown
)

type msgproducer interface {
	String() string
}

func dataMsgStreamTypeString(streamType dataMsgStreamType) string {

	switch streamType {
	case dMStreamGPB:
		return "GPB(compact)"
	case dMStreamGPBKV:
		return "GPB(k/v)"
	case dMStreamJSON:
		return "JSON"
	case dMStreamJSONEvents:
		return "JSON(events)"
	case dMStreamTemplate:
		return "template"
	}

	return "Unknown"
}

func dataMsgStreamTypeFromEncoding(enc encoding) (error, dataMsgStreamType) {

	mapping := map[encoding]dataMsgStreamType{
		ENCODING_GPB_COMPACT: dMStreamGPB,
		ENCODING_GPB_KV:      dMStreamGPBKV,
		ENCODING_JSON:        dMStreamJSON,
		ENCODING_JSON_EVENTS: dMStreamJSONEvents,
		ENCODING_GPB:         dMStreamGPB,
		ENCODING_TEMPLATE:    dMStreamTemplate,
	}

	s, ok := mapping[enc]
	if !ok {
		return fmt.Errorf("Unsupported encoding %v", enc),
			dMStreamMsgUnknown
	}

	return nil, s
}

func dataMsgStreamTypeToEncoding(dmt dataMsgStreamType) (error, encoding) {

	mapping := map[dataMsgStreamType]encoding{
		dMStreamGPB:   ENCODING_GPB,
		dMStreamJSON:  ENCODING_JSON,
		dMStreamGPBKV: ENCODING_GPB_KV, // legacy support
	}

	s, ok := mapping[dmt]
	if !ok {
		return fmt.Errorf("Unsupported dataMsgStreamType %v", dmt), ENCODING_MAX
	}

	return nil, s
}

//
// Specification of a dataMsg stream; type and context.
type dataMsgStreamSpec struct {
	streamType dataMsgStreamType
	context    interface{}
}

//
// A dataMsgStream defaulting to the native (input) type of a message
var dataMsgStreamSpecDefault = &dataMsgStreamSpec{
	streamType: dMStreamMsgDefault,
	context:    nil,
}

func (s *dataMsgStreamSpec) dataMsgStreamSpecTextBased() bool {
	switch s.streamType {
	case dMStreamJSON, dMStreamJSONEvents:
		return true
	default:
		return false
	}
}

//
// Extract specification o dataMsgStream from nodeconfig
func dataMsgStreamSpecFromConfig(
	nc nodeConfig,
	name string) (error, *dataMsgStreamSpec) {

	encodingString, err := nc.config.GetString(name, "encoding")
	if err != nil {
		// Default to JSON encoding
		encodingString = "json"
	}

	err, encoding := nameToEncoding(encodingString)
	if err != nil {
		return err, nil
	}

	err, streamType := dataMsgStreamTypeFromEncoding(encoding)
	if err != nil {
		return err, nil
	}

	spec := &dataMsgStreamSpec{
		streamType: streamType,
		context:    nil,
	}

	switch streamType {
	case dMStreamTemplate:

		//
		// Read the template name
		templateFileName, err := nc.config.GetString(name, "template")
		if err != nil {
			return fmt.Errorf(
				"encoding='template' requires a template option [%v]", err), nil
		}

		templateSpec, err := ioutil.ReadFile(templateFileName)
		if err != nil {
			return fmt.Errorf("read template file [%v]", err), nil
		}

		spec.context, err = template.New(name).Parse(string(templateSpec))
		if err != nil {
			return fmt.Errorf("parsing template [%v]", err), nil
		}

	default:

	}

	return nil, spec
}

// The heart of the pipeline is the dataMsg. This is what is carried
// around within the pipeline. The dataMsg is produced in some input
// stage, and eventually consumed and probably shipped out in some
// output stage.
//
// As the data message wends itself through the pipeline
//
//  - remember that the same msg is pushed onto multiple paths and
//  should be immutable.
//
//
type dataMsg interface {
	getDataMsgDescription() string
	produceByteStream(*dataMsgStreamSpec) (error, []byte)
	produceMetrics(*metricsSpec, metricsOutputHandler, metricsOutputContext) error
	getDataMsgStreamType() dataMsgStreamType
	getMetaDataPath() (error, string)
	getMetaDataIdentifier() (error, string)
	getMetaData() *dataMsgMetaData
}

//
// Concrete meta data can be returned from message types.
type dataMsgMetaData struct {
	Path       string
	Identifier string
}

//
// Control of the pipeline is achieved over control channels from the
// conductor to the nodes.
//
type msgID int

const (
	//
	// Used to request to shutdown, expects ACK on respChan
	SHUTDOWN msgID = iota
	// Request to report back on pipeline node state
	REPORT
	// Acknowledge a request.
	ACK
)

type msgStats struct {
	MsgsOK  uint64
	MsgsNOK uint64
}

//
// Control message channel type
//
type ctrlMsg struct {
	id       msgID
	content  []byte
	respChan chan *ctrlMsg
}
