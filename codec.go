//
// February 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Codec factory
//
package main

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
)

type encoding int

//
// There are dependencies on the this set of values starting from 0,
// and not skipping any values: (see getNewEncapSTParser)
const (
	ENCODING_GPB_COMPACT encoding = iota
	ENCODING_GPB_KV
	ENCODING_JSON
	//
	// ENCODING_JSON_EVENTS:
	// A format we produce (namely for 3rd parties to consume):
	// - if K/V content, un K/V it
	// - separate first level of fields into distinct events
	//
	ENCODING_JSON_EVENTS
	//
	// ENCODING_GPB:
	// A format which handles both GPB_KV and GPB_COMPACT
	ENCODING_GPB
	//
	// Template based encoding (only produced, not consumed, currently)
	ENCODING_TEMPLATE
	ENCODING_MAX
)

//
// Do we support receiving this content in telemetry? (as opposed to producing it)
var codec_support = []encoding{
	ENCODING_GPB,
	ENCODING_GPB_COMPACT,
	ENCODING_GPB_KV,
	ENCODING_JSON,
}

//
// Produce encoding for name
func nameToEncoding(encap string) (error, encoding) {

	mapping := map[string]encoding{
		"gpbcompact":  ENCODING_GPB_COMPACT,
		"gpbkv":       ENCODING_GPB_KV,
		"json":        ENCODING_JSON,
		"json_events": ENCODING_JSON_EVENTS,
		"gpb":         ENCODING_GPB,
		"template":    ENCODING_TEMPLATE,
	}

	encoding, ok := mapping[encap]
	if ok {
		return nil, encoding
	}

	err := fmt.Errorf(
		"encoding [%s], expected value from %v",
		encap, mapping)

	return err, encoding
}

//
// Produce name for encoding
func encodingToName(enc encoding) string {

	mapping := map[encoding]string{
		ENCODING_GPB_COMPACT: "gpbcompact",
		ENCODING_GPB_KV:      "gpbkv",
		ENCODING_JSON:        "json",
		ENCODING_JSON_EVENTS: "json_events",
		ENCODING_GPB:         "gpb",
		ENCODING_TEMPLATE:    "template",
	}

	return mapping[enc]
}

type codec interface {
	blockToDataMsgs(source msgproducer, nextBlock []byte) (error, []dataMsg)
	dataMsgToBlock(dM dataMsg) (error, []byte)
}

//
// Specific codec
func getCodec(name string, e encoding) (error, codec) {

	switch e {
	case ENCODING_GPB_COMPACT, ENCODING_GPB_KV, ENCODING_GPB:
		return getNewCodecGPB(name, e)
	case ENCODING_JSON:
		return getNewCodecJSON(name)

	}

	return fmt.Errorf("CODEC: codec unsupported"), nil
}

// Loaded once, and never changed. Exposed directly rather than
// through accessors.
var basePathXlation map[string]string

func codec_init(nc nodeConfig) {

	bpxFilename, err := nc.config.GetString("default", "base_path_xlation")
	if err != nil {
		return
	}

	logctx := logger.WithFields(log.Fields{
		"name":              "default",
		"base_path_xlation": bpxFilename,
	})

	bpxJSON, err := ioutil.ReadFile(bpxFilename)
	if err != nil {
		logctx.WithError(err).Error(
			"failed to read file containing base path translation map")
		return
	}

	err = json.Unmarshal(bpxJSON, &basePathXlation)
	if err != nil {
		logctx.WithError(err).Error(
			"failed to parse JSON describing base path translation")
		return
	}

	logctx.WithFields(
		log.Fields{"xlation_entries": len(basePathXlation)}).Info(
		"loaded base path translation map, applied on input")
}

type CodecMetaMonitorType struct {
	//
	// Number of messages decoded for a given codec. Note that a
	// message is that defined by the encap (e.g.  one frame in
	// the streaming telemetry format, or one grpc message in
	// grpc.)
	Decoded *prometheus.CounterVec
	//
	// Number of message bytes decoded for a given codec.
	DecodedBytes *prometheus.CounterVec
	//
	// Counter of messages  partitioned by base paths decoded
	// sufficiently to extract base path.
	BasePathGroups *prometheus.CounterVec
	//
	// Counter of errors per base path.
	BasePathDecodeError *prometheus.CounterVec
}

var codecMetaMonitor *CodecMetaMonitorType

func init() {
	//
	// We track messages decoded by codecs across a number of
	// dimensions. To that end the common codec sets up the
	// metrics.
	//
	codecMetaMonitor = &CodecMetaMonitorType{
		Decoded: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "codec_decoded_msgs",
				Help: "Number of messages decoded (partitioned)",
			},
			[]string{"section", "source", "codec"}),
		DecodedBytes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "codec_decoded_bytes",
				Help: "Number of bytes decoded (partitioned)",
			},
			[]string{"section", "source", "codec"}),
		BasePathGroups: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "codec_base_path_groups",
				Help: "Counter tracking groups per-base_path",
			},
			[]string{"section", "source", "base_path"}),
		BasePathDecodeError: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "codec_base_path_decode_error",
				Help: "Counter tracking decode errors per-base_path",
			},
			[]string{"section", "source", "base_path", "errortype"}),
	}

	prometheus.MustRegister(codecMetaMonitor.Decoded)
	prometheus.MustRegister(codecMetaMonitor.DecodedBytes)
	prometheus.MustRegister(codecMetaMonitor.BasePathGroups)
	prometheus.MustRegister(codecMetaMonitor.BasePathDecodeError)
}
