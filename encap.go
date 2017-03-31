//
// February 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Encap factory

package main

import (
	"fmt"
)

type encapParser interface {
	//
	// nextBlockBuffer, nextBlock
	// Based on the current state, provide the next fixed length
	// buffer to read. Note this type of encap works for encoding
	// where the length of the header or payload to collect next
	// is known a priori (typical of TLV encoding).
	//
	nextBlockBuffer() (error, *[]byte)
	nextBlock(nextBlock []byte, source msgproducer) (error, []dataMsg)
}

//
// Fetch and an encapParser for type (encap) corresponding to the
// encap, and indicate which pipeline node (name), and external
// producer involved (producer).
func getNewEncapParser(name string, encap string, msgproducer msgproducer) (
	error, encapParser) {

	switch encap {
	case "st":
		err, p := getNewEncapSTParser(name, msgproducer)
		return err, p
	}

	return fmt.Errorf("ENCAP: failed to produce parser"), nil
}
