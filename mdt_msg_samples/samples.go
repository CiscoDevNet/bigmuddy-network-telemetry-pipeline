//
// August 2016, Christian Cassar
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//

//
// Packages exporting message samples for test purposes.
package mdt_msg_samples

import (
	"bufio"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	telem "github.com/nleiva/telemetry-proto/proto_go"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"os"
	"strings"
)

type sampleTelemetryTable []SampleTelemetryTableEntry

type SampleTelemetryTableEntry struct {
	Sample             *telem.Telemetry
	SampleStreamGPB    []byte
	SampleStreamJSON   []byte
	SampleStreamJSONKV []byte
	Leaves             int
	Events             int
}

type SampleTelemetryDatabaseID int

const (
	SAMPLE_TELEMETRY_DATABASE_BASIC SampleTelemetryDatabaseID = iota
)

var sampleTelemetryDatabase map[SampleTelemetryDatabaseID]sampleTelemetryTable

func MDTSampleTelemetryTableFetchOne(
	dbindex SampleTelemetryDatabaseID) *SampleTelemetryTableEntry {

	if len(sampleTelemetryDatabase) <= int(dbindex) {
		return nil
	}

	table := sampleTelemetryDatabase[dbindex]
	return &table[0]
}

type MDTContext interface{}
type MDTSampleCallback func(sample *SampleTelemetryTableEntry, context MDTContext) (abort bool)

//
// MDTSampleTelemetryTableIterate iterates over table of samples
// calling caller with function MDTSampleCallback and opaque context
// MDTContext provided, for every known sample. The number of samples
// iterated over is returned.
func MDTSampleTelemetryTableIterate(
	dbindex SampleTelemetryDatabaseID,
	fn MDTSampleCallback,
	c MDTContext) (applied int) {

	if len(sampleTelemetryDatabase) <= int(dbindex) {
		return 0
	}
	count := 0
	table := sampleTelemetryDatabase[dbindex]
	for _, entry := range table {
		count++
		if fn(&entry, c) {
			break
		}
	}

	return count
}

func MDTLoadMetrics() string {
	b, e := ioutil.ReadFile("mdt_msg_samples/dump.metrics")
	if e == nil {
		return string(b)
	}
	return ""
}

func init() {

	sampleTelemetryDatabase = make(map[SampleTelemetryDatabaseID]sampleTelemetryTable)

	sampleTelemetryDatabase[SAMPLE_TELEMETRY_DATABASE_BASIC] = sampleTelemetryTable{}

	marshaller := &jsonpb.Marshaler{
		EmitDefaults: true,
		OrigName:     true,
	}

	kv, err := os.Open("dump.jsonkv")
	if err != nil {
		kv, err = os.Open("mdt_msg_samples/dump.jsonkv")
		if err != nil {
			log.Fatal(err)
		}
	}
	defer kv.Close()

	dump := bufio.NewReader(kv)
	decoder := json.NewDecoder(dump)

	_, err = decoder.Token()
	if err != nil {
		log.Fatal(err)
	}

	// Read the messages and build the db.
	for decoder.More() {
		var m telem.Telemetry

		err := jsonpb.UnmarshalNext(decoder, &m)
		if err != nil {
			log.Fatal(err)
		}

		gpbstream, err := proto.Marshal(&m)
		if err != nil {
			log.Fatal(err)
		}

		jsonstream, err := marshaller.MarshalToString(&m)
		if err != nil {
			log.Fatal(err)
		}

		entry := SampleTelemetryTableEntry{
			Sample:             &m,
			SampleStreamGPB:    gpbstream,
			SampleStreamJSONKV: json.RawMessage(jsonstream),
			Leaves:             strings.Count(jsonstream, "\"name\""),
			Events:             strings.Count(jsonstream, "\"content\""),
		}

		sampleTelemetryDatabase[SAMPLE_TELEMETRY_DATABASE_BASIC] =
			append(sampleTelemetryDatabase[SAMPLE_TELEMETRY_DATABASE_BASIC], entry)
	}

	jsondump, err := ioutil.ReadFile("dump.json")
	if err != nil {
		jsondump, err = ioutil.ReadFile("mdt_msg_samples/dump.json")
		if err != nil {
			// No validation if we don't have the results
			return
		}
	}
	var rows []json.RawMessage

	err = json.Unmarshal(jsondump, &rows)
	if err != nil {
		log.Fatal("Failed to unmarshall verification data")
	}
	for i, row := range rows {
		sample := &sampleTelemetryDatabase[SAMPLE_TELEMETRY_DATABASE_BASIC][i]
		sample.SampleStreamJSON = row
	}

	// log.Printf("%v", sampleTelemetryDatabase[SAMPLE_TELEMETRY_DATABASE_BASIC])
}
