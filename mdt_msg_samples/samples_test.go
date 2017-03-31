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
	"fmt"
	"testing"
)

func sampleDump(sample *SampleTelemetryTableEntry, context MDTContext) (abort bool) {
	if sample.SampleStreamJSON != nil {
		fmt.Printf("%+v\n", string(sample.SampleStreamJSON))
	}
	return false
}

func TestSampleDB(t *testing.T) {
	count := MDTSampleTelemetryTableIterate(SAMPLE_TELEMETRY_DATABASE_BASIC, sampleDump, nil)
	if count == 0 {
		t.Errorf("Sample database empty!")
	}
}
