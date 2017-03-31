//
// February 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Provide JSON codec, such as it is. More effort required here to
// exploit common bit (Telemetry message) and provide better
// implementations exporting metadata. Shipping MDT does not support
// JSON yet, though this is in the works. JSON is largely pass
// through.
//
package main

import (
	"fmt"
)

//
// dataMsgJSON dataMsg types are produced when handling JSON streams.
type dataMsgJSON struct {
	original []byte
	source   msgproducer
}

func (m *dataMsgJSON) getDataMsgDescription() string {
	_, id := m.getMetaDataIdentifier()
	return fmt.Sprintf("JSON message [%s msg len %d]", id, len(m.original))
}

func (m *dataMsgJSON) produceByteStream(streamSpec *dataMsgStreamSpec) (
	error, []byte) {

	switch streamSpec.streamType {
	case dMStreamJSON, dMStreamMsgDefault:
		return nil, m.original
	}

	//
	// We only support producing stream in JSON for this message
	// for the moment - this is because we have the encoded
	// variant at hand.
	return fmt.Errorf("JSON CODEC: reformat msg to [%s] is"+
		" not supported", dataMsgStreamTypeString(streamSpec.streamType)), nil
}

func (m *dataMsgJSON) produceMetrics(
	spec *metricsSpec,
	outputHandler metricsOutputHandler,
	outputContext metricsOutputContext) error {
	return fmt.Errorf("JSON CODEC: metric extraction unsupported")
}

func (m *dataMsgJSON) getDataMsgStreamType() dataMsgStreamType {
	return dMStreamJSON
}

func (m *dataMsgJSON) getMetaDataPath() (error, string) {
	return fmt.Errorf("JSON CODEC: path extraction is not supported"), ""
}

func (m *dataMsgJSON) getMetaDataIdentifier() (error, string) {
	return nil, m.source.String()
}

func (m *dataMsgJSON) getMetaData() *dataMsgMetaData {
	return &dataMsgMetaData{
		Path:       "unsupported",
		Identifier: m.source.String(),
	}
}

type codecJSON struct {
	name string
}

//
// Produce a JSON type codec
func getNewCodecJSON(name string) (error, codec) {
	c := &codecJSON{
		name: name,
	}

	return nil, c
}

func (p *codecJSON) dataMsgToBlock(dM dataMsg) (error, []byte) {
	return fmt.Errorf("CODEC JSON: only decoding is supported currently"),
		nil
}

// nextBlock allows JSON to produce dataMsg
func (p *codecJSON) blockToDataMsgs(source msgproducer, nextBlock []byte) (
	error, []dataMsg) {
	//
	// Count the decoded message against the source, section and
	// type
	codecMetaMonitor.Decoded.WithLabelValues(
		p.name, source.String(), encodingToName(ENCODING_JSON)).Inc()
	codecMetaMonitor.DecodedBytes.WithLabelValues(
		p.name, source.String(), encodingToName(ENCODING_JSON)).Add(
		float64(len(nextBlock)))

	dMs := make([]dataMsg, 1)
	dMs[0] = &dataMsgJSON{
		original: nextBlock,
		source:   source,
	}

	return nil, dMs
}
