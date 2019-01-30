//
// August 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	samples "github.com/cisco/bigmuddy-network-telemetry-pipeline/mdt_msg_samples"
	telem "github.com/nleiva/telemetry-proto/proto_go"
	"github.com/dlintw/goconf"
	"github.com/golang/protobuf/jsonpb"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestCodecGPB2JSON(t *testing.T) {

	var targetString, targetNumber telem.Telemetry
	var generic interface{}

	source := &telem.Telemetry{
		CollectionId: 1234567890,
	}
	marshallerEmitNumber := &jsonpb.Marshaler{
		EmitUInt64Unquoted: true,
		EmitDefaults:       true,
		OrigName:           true,
	}

	marshallerEmitString := &jsonpb.Marshaler{
		EmitUInt64Unquoted: false,
		EmitDefaults:       true,
		OrigName:           true,
	}

	jsonNumber, err := marshallerEmitNumber.MarshalToString(source)
	if err != nil {
		t.Fatalf("Failed to jsonify (uint64 -> number) [%v]", err)
	}
	fmt.Println("jsonNumber: ", jsonNumber)

	jsonString, err := marshallerEmitString.MarshalToString(source)
	if err != nil {
		t.Fatalf("Failed to jsonify (uint64 -> string) [%v]", err)
	}
	fmt.Println("jsonString: ", jsonString)

	Unmarshaler := &jsonpb.Unmarshaler{AllowUnknownFields: true}
	err = Unmarshaler.Unmarshal(bytes.NewBuffer([]byte(jsonString)),
		&targetString)
	if err != nil {
		t.Fatalf("Failed to unjsonify (uint64 -> string) [%v]", err)
	}

	err = Unmarshaler.Unmarshal(bytes.NewBuffer([]byte(jsonNumber)),
		&targetNumber)
	if err != nil {
		t.Fatalf("Failed to unjsonify (uint64 -> number) [%v]", err)
	}

	//
	// Compare source and targets
	if targetNumber.CollectionId != targetString.CollectionId ||
		targetString.CollectionId != source.CollectionId {
		t.Fatalf("Extracted differences between source, target String/Number")
	}
	//
	// Generic decoding is where EmitUInt64Unquoted is useful
	err = json.Unmarshal([]byte(jsonString), &generic)
	if err != nil {
		t.Fatalf("Failed generic unmarshal (uint64 -> string) [%v]", err)
	}
	genericMap := generic.(map[string]interface{})
	v := genericMap["collection_id"]
	switch v.(type) {
	case string:
		//
		// as expected
	default:
		t.Fatalf("Failed unmarshal unexpected type (uint64 -> string): %T [%v]",
			v, err)
	}

	//
	// Generic decoding is where EmitUInt64Unquoted is useful
	err = json.Unmarshal([]byte(jsonNumber), &generic)
	if err != nil {
		t.Fatalf("Failed generic unmarshal (uint64 -> number) [%v]", err)
	}
	genericMap = generic.(map[string]interface{})
	v = genericMap["collection_id"]
	switch v.(type) {
	case float64:
		//
		// as expected
	default:
		t.Fatalf("Failed unmarshal unexpected type (uint64 -> number): %T", v)
	}

}

type gpbTestControlTemplate struct {
	t          *testing.T
	codec      codec
	streamSpec *dataMsgStreamSpec
}

func (m *gpbTestControlTemplate) String() string {
	return "codec_gpb_test_template"
}

func codecGPBProcessSampleTemplate(
	sample *samples.SampleTelemetryTableEntry,
	context samples.MDTContext) (abort bool) {

	control := context.(gpbTestControlTemplate)

	err, msgs := control.codec.blockToDataMsgs(&control, sample.SampleStreamGPB)
	if err != nil {
		fmt.Printf("Failed to extract messages from GPB stream: [%v]\n", err)
	}

	for _, msg := range msgs {
		err, b := msg.produceByteStream(control.streamSpec)
		if err != nil {
			control.t.Fatal(err)
		}
		fmt.Printf("%s", string(b))
	}

	return false
}

func TestCodecGPBTemplate(t *testing.T) {

	var nc nodeConfig

	err, codec := getNewCodecGPB("test", ENCODING_GPB)
	if err != nil {
		t.Fatalf("Failed to get a GPB codec to test [%v]", err)
	}
	nc.config, err = goconf.ReadConfigFile("pipeline_test.conf")
	if err != nil {
		t.Fatalf("Failed to get template config [%v]", err)
	}

	err, streamSpec := dataMsgStreamSpecFromConfig(nc, "templatetest")
	if err != nil {
		t.Fatalf("Failed to parse template [%v]", err)
	}

	control := gpbTestControlTemplate{
		t:          t,
		codec:      codec,
		streamSpec: streamSpec,
	}

	count := samples.MDTSampleTelemetryTableIterate(
		samples.SAMPLE_TELEMETRY_DATABASE_BASIC,
		codecGPBProcessSampleTemplate,
		control)
	if count == 0 {
		t.Errorf("No iterations run")
	}
}

type metricsTestOutputHandler struct {
	t     *testing.T
	b     *testing.B
	buf   *bytes.Buffer
	codec codec
	spec  *metricsSpec
}

// Issue test or benchmark failure
func (m *metricsTestOutputHandler) errorFn(failure string) {
	if m.t != nil {
		m.t.Errorf(failure)
	} else if m.b != nil {
		m.b.Errorf(failure)
	}
}

//
// Test constrains symbols in sensor name
func (p *metricsTestOutputHandler) adaptSensorName(name string) string {
	return name
}

//
// Test constrains symbols in tag names
func (p *metricsTestOutputHandler) adaptTagName(name string) string {
	return name
}

func (p *metricsTestOutputHandler) flushMetric(
	tags []metricsAtom,
	ts uint64,
	buf metricsOutputContext) {
}

func (p *metricsTestOutputHandler) buildMetric(
	tags []metricsAtom,
	sensor metricsAtom,
	ts uint64,
	context metricsOutputContext) {

	if p.buf == nil {
		// Benchmarking. Bail out asap
		return
	}

	buf := context.(*bytes.Buffer)
	buf.WriteString(
		fmt.Sprintf("{\"tags\": \"%v\", \"metrics\":\"%v\",\"ts\":\"%v\"}\n",
			tags, sensor, ts))
}

func (p *metricsTestOutputHandler) setupWorkers(m *metricsOutputModule) {
}

func (m *metricsTestOutputHandler) String() string {
	return "codec_gpb_benchmark"
}

func codecGPBExtractMetrics(
	sample *samples.SampleTelemetryTableEntry,
	context samples.MDTContext) (abort bool) {

	c := context.(*metricsTestOutputHandler)
	err, msgs := c.codec.blockToDataMsgs(c, sample.SampleStreamGPB)
	if err != nil {
		c.errorFn(
			fmt.Sprintf(
				"Failed to extract messages from GPB metrics: [%v]\n", err))
	}

	for _, msg := range msgs {
		err = msg.produceMetrics(c.spec, c, c.buf)
		if err != nil {
			c.errorFn(
				fmt.Sprintf(
					"Error extract metric: %s [%v]\n",
					msg.getDataMsgDescription(), err))
		}
	}

	return false
}
func TestCodecGPBMetrics(t *testing.T) {

	var nc nodeConfig

	err, codec := getNewCodecGPB("test", ENCODING_GPB)
	if err != nil {
		t.Errorf("Failed to get a GPB codec to test")
	}

	//
	// Will produce metrics from samples, into buf and then compare
	// output against expected outcome.
	c := &metricsTestOutputHandler{
		t:     t,
		buf:   new(bytes.Buffer),
		codec: codec,
	}

	testOutputHandler = c
	nc.config, err = goconf.ReadConfigFile("pipeline_test.conf")
	if err != nil {
		t.Errorf("Metrics GPB codec, pipeline test config missing\n")
	}

	m := &metricsOutputModule{}

	m.configure("mymetricstest", nc)
	c.spec = &m.inputSpec

	samples.MDTSampleTelemetryTableIterate(
		samples.SAMPLE_TELEMETRY_DATABASE_BASIC,
		codecGPBExtractMetrics, c)

	expect := samples.MDTLoadMetrics()
	if len(expect) == 0 {
		t.Errorf("Failed to load expected results\n")
	}
	cmp := strings.Compare(c.buf.String(), expect)
	if cmp != 0 {
		t.Errorf("Metrics GPB codec, pipeline test comp failed\n")
		t.Errorf("Found:\n%v\n", c.buf.String())
		t.Errorf("Expected:\n%v\n", samples.MDTLoadMetrics())
		// If you are sure results are good... rewrite results file
		//ioutil.WriteFile("mdt_msg_samples/dump.metrics",
		//	c.buf.Bytes(), os.ModePerm)
	}

}

type gpbTestControl struct {
	t                 *testing.T
	b                 *testing.B
	codec             codec
	produceByteStream bool
	validate          bool
	streamType        dataMsgStreamType
}

// compareJSON - compares content by unmarshaling to a map and using
// reflection to deep compare the map.
func compareJSON(a []byte, b []byte) bool {
	type mapping map[string]interface{}

	var map_a = make(mapping)
	var map_b = make(mapping)

	err := json.Unmarshal(a, &map_a)
	if err != nil {
		return false
	}

	err = json.Unmarshal(b, &map_b)
	if err != nil {
		return false
	}

	fmt.Printf("\n===A===\n%+v\n", map_a)
	fmt.Printf("\n===B===\n%+v\n", map_b)
	return reflect.DeepEqual(map_a, map_b)
}

// Issue test or benchmark failure
func (g *gpbTestControl) errorFn(failure string) {
	if g.t != nil {
		g.t.Errorf(failure)
	} else if g.b != nil {
		g.b.Errorf(failure)
	}
}

func (g *gpbTestControl) validateFn(
	sample *samples.SampleTelemetryTableEntry,
	b []byte) error {

	switch g.streamType {
	case dMStreamGPB, dMStreamMsgDefault:
		// Compare b to sample
		if !bytes.Equal(b, sample.SampleStreamGPB) {
			return fmt.Errorf("Original stream and produced stream did not match")
		}
	case dMStreamJSON:
		if false {
			//
			// If we are sure things are working as expected write the
			// content.
			f, err := os.OpenFile("mdt_msg_samples/dump.json",
				os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			if _, err = f.Write(b); err != nil {
				panic(err)
			}
		} else {
			if sample.SampleStreamJSON != nil {
				if !strings.Contains(string(b), string(sample.SampleStreamJSON)) &&
					!compareJSON(b, sample.SampleStreamJSON) {
					return fmt.Errorf("JSON stream and produced JSON did not match")
				}
			}
		}
	case dMStreamJSONEvents:
		events := strings.Count(string(b), "ontent")
		if events != sample.Events {
			return fmt.Errorf("Count of events [%d] does not match original [%d]",
				events, sample.Events)
		}
	default:
	}

	return nil
}

func (m *gpbTestControl) String() string {
	return "codec_gpb_test"
}

func codecGPBProcessSampleDescribe(
	sample *samples.SampleTelemetryTableEntry,
	context samples.MDTContext) (abort bool) {

	control := context.(gpbTestControl)

	fmt.Printf("===Sample start:\n   Objects %d, Leaves %d\n", sample.Events, sample.Leaves)

	err, msgs := control.codec.blockToDataMsgs(&control, sample.SampleStreamGPB)
	if err != nil {
		fmt.Printf("Failed to extract messages from GPB stream: [%v]\n", err)
	}

	for _, msg := range msgs {
		fmt.Printf("   Msg: %s\n", msg.getDataMsgDescription())
	}

	fmt.Printf("===Sample End\n")

	return false
}

func codecGPBProcessSample(
	sample *samples.SampleTelemetryTableEntry,
	context samples.MDTContext) (abort bool) {

	control := context.(gpbTestControl)

	err, msgs := control.codec.blockToDataMsgs(&control, sample.SampleStreamGPB)
	if err != nil {
		fmt.Printf("Failed to extract messages from GPB stream: [%v]\n", err)
	}

	if control.produceByteStream {
		for _, msg := range msgs {
			err, b := msg.produceByteStream(
				&dataMsgStreamSpec{streamType: control.streamType})
			if err != nil {
				if control.t != nil {
					control.errorFn(
						fmt.Sprintf("Failed to produce byte stream for msg of type %v: [%v]\n",
							control.streamType,
							err))
				}
			}
			if len(b) == 0 {
				control.t.Errorf("Zero length byte stream\n")
			}

			if control.validate {
				err = control.validateFn(sample, b)
				if err != nil {
					control.errorFn(fmt.Sprintf("Validation failed: [%v]", err))
				}
			}
		}
	}
	return false
}

func TestCodecGPBDescribe(t *testing.T) {

	err, codec := getNewCodecGPB("test", ENCODING_GPB)

	if err != nil {
		t.Errorf("Failed to get a GPB codec to test")
	}

	control := gpbTestControl{
		t:                 t,
		codec:             codec,
		produceByteStream: false,
	}

	samples.MDTSampleTelemetryTableIterate(
		samples.SAMPLE_TELEMETRY_DATABASE_BASIC,
		codecGPBProcessSampleDescribe,
		control)
}

func TestCodecGPBBasic(t *testing.T) {

	err, codec := getNewCodecGPB("test", ENCODING_GPB)

	if err != nil {
		t.Errorf("Failed to get a GPB codec to test")
	}

	var permutations = []struct {
		produce    bool
		validate   bool
		streamType dataMsgStreamType
	}{
		{false, false, dMStreamGPB},
		{true, false, dMStreamGPB},
		{true, true, dMStreamGPB},
		{true, true, dMStreamJSONEvents},
		{true, true, dMStreamJSON},
	}

	for _, p := range permutations {
		control := gpbTestControl{
			t:                 t,
			codec:             codec,
			produceByteStream: p.produce,
			validate:          p.validate,
			streamType:        p.streamType,
		}

		count := samples.MDTSampleTelemetryTableIterate(
			samples.SAMPLE_TELEMETRY_DATABASE_BASIC,
			codecGPBProcessSample,
			control)
		if count == 0 {
			t.Errorf("No iterations run")
		}
	}
}

func codecGPBBenchmarkHelper(b *testing.B, control gpbTestControl) {

	var err error

	err, control.codec = getNewCodecGPB("test", ENCODING_GPB)
	if err != nil {
		b.Errorf("Failed to get a GPB codec to test")
	}

	control.b = b

	//
	// Start benchmark clock now
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		count := samples.MDTSampleTelemetryTableIterate(
			samples.SAMPLE_TELEMETRY_DATABASE_BASIC,
			codecGPBProcessSample,
			control)
		if count == 0 {
			b.Errorf("No iterations run")
		}
	}
}

func BenchmarkCodecGPBCreateDM(b *testing.B) {
	codecGPBBenchmarkHelper(b, gpbTestControl{
		produceByteStream: false,
		validate:          false,
		streamType:        dMStreamGPB,
	})
}

func BenchmarkCodecGPBCreateDMProduceGPB(b *testing.B) {
	codecGPBBenchmarkHelper(b, gpbTestControl{
		produceByteStream: true,
		validate:          false,
		streamType:        dMStreamGPB,
	})
}

func BenchmarkCodecGPBCreateDMProduceJSON(b *testing.B) {
	codecGPBBenchmarkHelper(b, gpbTestControl{
		produceByteStream: true,
		validate:          false,
		streamType:        dMStreamJSON,
	})
}

func BenchmarkCodecGPBCreateDMProduceJSONEvents(b *testing.B) {
	codecGPBBenchmarkHelper(b, gpbTestControl{
		produceByteStream: true,
		validate:          false,
		streamType:        dMStreamJSONEvents,
	})
}

func BenchmarkCodecGPBMetrics(b *testing.B) {

	var nc nodeConfig

	err, codec := getNewCodecGPB("test", ENCODING_GPB)
	if err != nil {
		b.Errorf("Failed to get a GPB codec to test")
	}

	c := &metricsTestOutputHandler{
		codec: codec,
	}

	testOutputHandler = c
	nc.config, err = goconf.ReadConfigFile("pipeline_test.conf")
	if err != nil {
		b.Errorf("Metrics GPB codec, pipeline test config missing\n")
	}

	m := &metricsOutputModule{}

	m.configure("mymetricstest", nc)
	c.spec = &m.inputSpec

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		samples.MDTSampleTelemetryTableIterate(
			samples.SAMPLE_TELEMETRY_DATABASE_BASIC,
			codecGPBExtractMetrics, c)
	}
}
