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
	"testing"
)

func TestCodecJSON(t *testing.T) {

	var codecJSONTestSource testSource

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

	err, b := p.dataMsgToBlock(dM)
	if err == nil {
		t.Errorf("Unexpected unsupported")
	}

	err, _ = dM.getMetaDataIdentifier()
	if err != nil {
		t.Errorf("Failed to retrieve identifier [%v]", err)
	}

	description := dM.getDataMsgDescription()
	if description == "" {
		t.Errorf("Failed to retrieve description")
	}

	err, _ = dM.getMetaDataPath()
	if err == nil {
		t.Errorf("Unexpected unsupported, no path to fetch")
	}

	err, b = dM.produceByteStream(&dataMsgStreamSpec{streamType: dMStreamGPB})
	if err == nil {
		t.Errorf("Unexpected unsupported GPB byte stream from JSON")
	}

	err, b = dM.produceByteStream(dataMsgStreamSpecDefault)
	if err != nil {
		t.Errorf("Failed to produce byte stream from dataMsg [%v]",
			err)
	}

	if bytes.Compare(b, testJSONMsg) != 0 {
		t.Errorf("Failed to extract expected data")
	}

}
