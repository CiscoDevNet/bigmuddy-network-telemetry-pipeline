//
// February 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Handle Streaming Telemetry Header.

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	ENC_ST_MAX_DGRAM          uint32 = 64 * 1024
	ENC_ST_MAX_PAYLOAD        uint32 = 1024 * 1024
	ENC_ST_HDR_MSG_FLAGS_NONE uint16 = 0
	ENC_ST_HDR_MSG_SIZE       uint32 = 12
	ENC_ST_HDR_VERSION        uint16 = 1
)

type encapSTHdrMsgType uint16

const (
	ENC_ST_HDR_MSG_TYPE_UNSED encapSTHdrMsgType = iota
	ENC_ST_HDR_MSG_TYPE_TELEMETRY_DATA
	ENC_ST_HDR_MSG_TYPE_HEARTBEAT
)

type encapSTHdrMsgEncap uint16

const (
	ENC_ST_HDR_MSG_ENCAP_UNSED encapSTHdrMsgEncap = iota
	ENC_ST_HDR_MSG_ENCAP_GPB
	ENC_ST_HDR_MSG_ENCAP_JSON
	ENC_ST_HDR_MSG_ENCAP_GPB_COMPACT
	ENC_ST_HDR_MSG_ENCAP_GPB_KV
)

type encapSTHdr struct {
	MsgType       encapSTHdrMsgType
	MsgEncap      encapSTHdrMsgEncap
	MsgHdrVersion uint16
	Msgflag       uint16
	Msglen        uint32
}

type encapSTParseState int

const (
	ENC_ST_WAIT_FOR_HDR encapSTParseState = iota
	ENC_ST_WAIT_FOR_DATA
	ENC_ST_WAIT_FOR_ALL // datagram service
)

type encapSTParser struct {
	name           string
	codecs         []codec
	hdr            []byte
	cachedMsgEncap encapSTHdrMsgEncap
	nextCodec      codec
	nextBlockSize  uint32
	state          encapSTParseState
	// Reference to the source field is passed down channels in
	// messages so it is important to remember it is immutable
	source msgproducer
}

func encapSTFromEncoding(enc encoding) (error, encapSTHdrMsgEncap) {
	var err error
	var encst encapSTHdrMsgEncap

	switch enc {
	case ENCODING_GPB:
		encst = ENC_ST_HDR_MSG_ENCAP_GPB
	case ENCODING_JSON:
		encst = ENC_ST_HDR_MSG_ENCAP_JSON
	case ENCODING_GPB_KV:
		encst = ENC_ST_HDR_MSG_ENCAP_GPB_KV // legacy support
	default:
		err = fmt.Errorf("Failed to produce encapSTHdrMsgEncap from %d", enc)
	}

	return err, encst
}

func getNewEncapSTParser(name string, source msgproducer) (error, encapParser) {

	var hdr encapSTHdr

	p := &encapSTParser{
		hdr:    make([]byte, binary.Size(hdr)),
		state:  ENC_ST_WAIT_FOR_HDR,
		source: source,
		name:   name,
	}

	if source == nil {
		//
		// This is a datagram service, not a connection oriented
		// service.
		p.state = ENC_ST_WAIT_FOR_ALL
	}

	//
	// Setup all possible codecs or fail
	p.codecs = make([]codec, ENCODING_MAX)
	for _, e := range codec_support {
		err, codec := getCodec(name, e)
		if err != nil {
			return err, nil
		}
		p.codecs[e] = codec
	}

	return nil, p
}

//
// Buffer for next block
func (p *encapSTParser) nextBlockBuffer() (error, *[]byte) {

	switch p.state {

	case ENC_ST_WAIT_FOR_DATA:

		if p.nextBlockSize == 0 {
			return fmt.Errorf("ENCAP  ST: req 0 size buffer"), nil
		}

		//
		// Extra safe. This check is not strictly necessary, because we
		// checked when we set it, but...
		if p.nextBlockSize > ENC_ST_MAX_PAYLOAD {
			return fmt.Errorf("ENCAP  ST: req %d size buf, max %d",
				p.nextBlockSize, ENC_ST_MAX_PAYLOAD), nil
		}

		buffer := make([]byte, p.nextBlockSize)
		return nil, &buffer

	case ENC_ST_WAIT_FOR_HDR:
		return nil, &p.hdr

	case ENC_ST_WAIT_FOR_ALL:
		//
		// Length of datagram is unknown. Take worst case. We must do
		// better somehow here.
		buffer := make([]byte, ENC_ST_MAX_DGRAM)
		return nil, &buffer
	}

	return fmt.Errorf(
		"ENCAP ST: parser in unknown state, buffer req"), nil
}

//
// cacheCodec caches the codec in use for encap parser, or validates
// and uses it.
func (p *encapSTParser) cacheCodec(encap encapSTHdrMsgEncap) error {

	//fmt.Printf("HDR: %+v\n", hdr)
	if p.nextCodec == nil || encap != p.cachedMsgEncap {
		//
		// Setup next codec.
		switch encap {
		case ENC_ST_HDR_MSG_ENCAP_GPB:
			p.nextCodec = p.codecs[ENCODING_GPB]
		case ENC_ST_HDR_MSG_ENCAP_GPB_COMPACT:
			p.nextCodec = p.codecs[ENCODING_GPB_COMPACT]
		case ENC_ST_HDR_MSG_ENCAP_GPB_KV:
			p.nextCodec = p.codecs[ENCODING_GPB_KV]
		case ENC_ST_HDR_MSG_ENCAP_JSON:
			p.nextCodec = p.codecs[ENCODING_JSON]
		default:
			return fmt.Errorf("ENCAP ST: no codec for msg encap [%+v]",
				encap)
		}
		p.cachedMsgEncap = encap
	} else {
		// This is the common path in a live session
		// since the codec to use would be cached.
		//
	}

	return nil
}

//
// Given requested amount of data, return next block size, and
// possibly a dataMsg
func (p *encapSTParser) nextBlock(nextBlock []byte, source msgproducer) (
	error, []dataMsg) {

	if source != nil {
		//
		// Source overwritten per msg (handling datagrams)
		p.source = source
	}
	switch p.state {

	case ENC_ST_WAIT_FOR_ALL:
		//
		// Datagram service.  Message is completely read into the
		// buffer already. All we need to do is validate content,
		// and use it for dM.
		var hdr encapSTHdr
		hdrbuf := bytes.NewReader(nextBlock)
		err := binary.Read(hdrbuf, binary.BigEndian, &hdr)
		if err != nil {
			return err, nil
		}

		//
		// We could relax this, but at the moment we only choose to
		// support one encoding on a given port. Different encodings
		// need to use different input sections, with different port
		// to listen on.
		err = p.cacheCodec(hdr.MsgEncap)
		if err != nil {
			return err, nil
		}

		//
		// Make sure msglen is sensible. hdrbuf.Len() is the unread
		// bit.
		hdrLen := len(nextBlock) - hdrbuf.Len()
		if hdr.Msglen > uint32(hdrbuf.Len()) {
			return fmt.Errorf(
				"ENCAP ST: drop datagram, payload len expect %d, have %d",
				hdr.Msglen, hdrbuf.Len()), nil
		}

		//
		// Unsupported flags?
		if hdr.Msgflag != ENC_ST_HDR_MSG_FLAGS_NONE {
			return fmt.Errorf(
				"ENCAP ST: flag in header unsupported (zlib?)"), nil
		}

		//
		// We have a codec otherwise we would have returned after
		// cacheCodec call. Extract payload and feed it to codec.
		return p.nextCodec.blockToDataMsgs(p.source, nextBlock[hdrLen:])

	case ENC_ST_WAIT_FOR_DATA:
		//
		// Track state, and return empty handed (no data yet)
		p.nextBlockSize = 0
		p.state = ENC_ST_WAIT_FOR_HDR

		if p.nextCodec != nil {
			return p.nextCodec.blockToDataMsgs(p.source, nextBlock)
		}

		return fmt.Errorf("ENCAP ST: codec not setup for decode"), nil

	case ENC_ST_WAIT_FOR_HDR:

		var hdr encapSTHdr
		hdrbuf := bytes.NewReader(nextBlock)
		err := binary.Read(hdrbuf, binary.BigEndian, &hdr)
		if err != nil {
			return err, nil
		}

		err = p.cacheCodec(hdr.MsgEncap)
		if err != nil {
			return err, nil
		}

		if hdr.Msglen > ENC_ST_MAX_PAYLOAD {
			return fmt.Errorf(
				"ENCAP ST: nextBlockBuffer failed; msg too long [%v]", hdr.Msglen), nil
		}

		if hdr.Msgflag != ENC_ST_HDR_MSG_FLAGS_NONE {
			return fmt.Errorf(
				"ENCAP ST: flag in header unsupported (zlib?)"), nil
		}

		//
		// Track state, and return empty handed (no data yet)
		p.nextBlockSize = hdr.Msglen
		p.state = ENC_ST_WAIT_FOR_DATA

		return nil, nil
	}

	return fmt.Errorf(
		"ENCAP ST: parser in unknown state, handling block"), nil
}
