//
// February 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Provide GPB (compact and K/V) encode/decode services.
//

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	telem "github.com/cisco/bigmuddy-network-telemetry-proto/proto_go"
	pdt "github.com/cisco/bigmuddy-network-telemetry-proto/proto_go/old/telemetry"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
)

const (
	GPBCODEC                       = "GPBCodec"
	CODEC_GPB_PIPELINE_EDIT_SUFFIX = "_PIPELINE_EDIT"
	CODEC_GPB_PREALLOC_PDT_ROWS    = 32
)

type dataMsgGPB struct {
	original []byte
	source   msgproducer
	//
	// Top level cached decode.
	cachedDecode *telem.Telemetry
	//
	// Decoded tbl; applicable only to GPB. For GPBK/V, the top level
	// decode completes the effort. Note that we use atomic.Value for
	// the cache type, with true type of *gpbDecodeTbl. This type is
	// set once, and once it is, then it is never reset. We only lock
	// while we're building to avoid unnecessarily decoding multiple
	// concurrent time, not for protection (note we use atomic
	// load/store).
	cachedDecodeTbl     atomic.Value
	cachedDecodeTblLock sync.Mutex
}

//
// Track
type gpbDecodeTbl struct {
	rows []*gpbDecodeRow
}

type gpbDecodeRow struct {
	Timestamp uint64
	Keys      proto.Message
	Content   proto.Message
}

func (m *dataMsgGPB) getDataMsgStreamType() dataMsgStreamType {
	return dMStreamGPB
}

func (m *dataMsgGPB) getMetaDataPath() (error, string) {
	return nil, m.cachedDecode.EncodingPath
}

func (m *dataMsgGPB) getMetaDataIdentifier() (error, string) {
	return nil, m.source.String()
}

func (m *dataMsgGPB) getMetaData() *dataMsgMetaData {
	return &dataMsgMetaData{
		Path:       m.cachedDecode.EncodingPath,
		Identifier: m.source.String(),
	}
}

func (m *dataMsgGPB) getDataMsgDescription() string {

	var node_id string

	_, source := m.getMetaDataIdentifier()
	if m.cachedDecode != nil {
		node_id = m.cachedDecode.GetNodeIdStr()
	} else {
		node_id = "n/a"
	}

	_, path := m.getMetaDataPath()
	return fmt.Sprintf("GPB(common) Message [%s(%s)/%s msg len: %d]",
		source, node_id, path, len(m.original))
}

func (p *codecGPB) dataMsgToBlock(dM dataMsg) (error, []byte) {
	return fmt.Errorf("CODEC GPB: only decoding is supported currently"),
		nil
}

//
// Support function to extract the value field from K/V Field.
func extractGPBKVNativeTypeFromOneof(
	field *telem.TelemetryField,
	must_be_numeric bool) interface{} {

	switch field.ValueByType.(type) {
	case *telem.TelemetryField_BytesValue:
		if !must_be_numeric {
			return field.GetBytesValue()
		}
	case *telem.TelemetryField_StringValue:
		if !must_be_numeric {
			return field.GetStringValue()
		}
	case *telem.TelemetryField_BoolValue:
		if !must_be_numeric {
			return field.GetBoolValue()
		}
	case *telem.TelemetryField_Uint32Value:
		return field.GetUint32Value()
	case *telem.TelemetryField_Uint64Value:
		return field.GetUint64Value()
	case *telem.TelemetryField_Sint32Value:
		return field.GetSint32Value()
	case *telem.TelemetryField_Sint64Value:
		return field.GetSint64Value()
	case *telem.TelemetryField_DoubleValue:
		return field.GetDoubleValue()
	case *telem.TelemetryField_FloatValue:
		return field.GetFloatValue()
	}

	return nil
}

func telemetryKvToSockDrawer(item []*telem.TelemetryField) *sockDrawer {
	var s sockDrawer

	if item == nil || len(item) == 0 {
		return nil
	}

	s = make(sockDrawer, 30) // Hint, of the number of fields

	placeInArrayMap := map[string]bool{}

	for _, field := range item {

		var fieldVal interface{}
		var hint int

		//
		// Strictly speaking we should assert that no field name passed in
		// has CODEC_GPB_PIPELINE_EDIT_SUFFIX to avoid possibly mistaking
		// it for one of ours

		existing_entry, exists := s[field.Name]
		_, placeInArray := placeInArrayMap[field.Name]

		children := field.GetFields()
		if children == nil {
			fieldVal = extractGPBKVNativeTypeFromOneof(field, false)
			hint = 10
		} else {
			fieldVal = telemetryKvToSockDrawer(children)

			hint = len(children)
		}

		if !placeInArray && !exists {
			//
			// this is the common case by far!
			s[field.Name] = fieldVal
		} else {
			newName := field.Name + CODEC_GPB_PIPELINE_EDIT_SUFFIX
			if exists {
				//
				// Play safe. If entry exists, placeInArray should be
				// false, because once placeInArray becomes true, the
				// s[field.Name] will NOT be populated.
				if !placeInArray {
					// Create list
					s[newName] = make([]interface{}, 0, hint)
					// Remember that this field name is arrayified(!?)
					placeInArrayMap[field.Name] = true
					// Add existing entry to new array)
					s[newName] = append(s[newName].([]interface{}),
						existing_entry)
					// Delete existing entry from old
					delete(s, field.Name)
					placeInArray = true
				} else {
					log.WithFields(log.Fields{
						"FieldName": field.Name,
					}).Error(
						"GPB KV inconsistency, processing repeated field names")
				}
			}
			if placeInArray && fieldVal != nil {
				s[newName] = append(s[newName].([]interface{}), fieldVal)
			}
		}
	}

	return &s
}

//
// Anything goes in a sockDrawer. This is a name-to-opaque map
// container where we collect content before marshalling into JSON or
// filtering in template, without necessarily knowing the content
// type.
type sockDrawer map[string]interface{}

type rowToFilter struct {
	Timestamp uint64
	Keys      interface{}
	Content   interface{}
}

type msgToFilter struct {
	Node_id               string
	Encoding_path         string
	Subscription          string
	collection_id         uint64
	collection_start_time uint64
	collection_end_time   uint64
	Msg_timestamp         uint64
	Data                  []rowToFilter
}

func (m *msgToFilter) populateDataFromGPB(s *telem.Telemetry) error {

	m.Node_id = s.GetNodeIdStr()
	m.Subscription = s.GetSubscriptionIdStr()
	m.Encoding_path = s.EncodingPath
	m.collection_id = s.CollectionId
	m.collection_start_time = s.CollectionStartTime
	m.collection_end_time = s.CollectionEndTime
	m.Msg_timestamp = s.MsgTimestamp

	compactGPBTable := s.GetDataGpb()
	if compactGPBTable != nil {
		//
		// native gpb support to follow
		return fmt.Errorf("No support for native gpb msgToFilter")
	} else {
		//
		// We need to unk/v the content before we JSONify.
		topfields := s.GetDataGpbkv()
		m.Data = make([]rowToFilter, len(topfields))
		i := 0
		for _, topfield := range topfields {
			rtf := &m.Data[i]
			i++
			rtf.Timestamp = topfield.Timestamp
			for _, kcfield := range topfield.GetFields() {
				// We should be iterating over two fields at this
				//level; keys and content.
				fields := kcfield.GetFields()
				if len(fields) > 0 {
					if kcfield.Name == "keys" {
						rtf.Keys = telemetryKvToSockDrawer(fields)
					} else if kcfield.Name == "content" {
						rtf.Content = telemetryKvToSockDrawer(fields)
					}
				} else {
					return fmt.Errorf("message with no row, nothing to filter")
				}
			}
		}
	}

	return nil
}

//
// Message row type used for serialisation
type rowToSerialise struct {
	Timestamp uint64
	Keys      *json.RawMessage
	Content   *json.RawMessage
}

//
// Message type (including header and rows) used for serialisation
type msgToSerialise struct {
	Source    string
	Telemetry *json.RawMessage
	Rows      []*rowToSerialise `json:"Rows,omitempty"`
}

//
// Produce byte stream from GPB K/V encoded content in preparation for
// JSON events.  Eventually, we should cache the decoded content to
// avoid multiple output stages having to decode original multeiple
// times. This is easy, but will require locking access to data
// message cache.
//
// Much as with codecGPBJSONifyDataGPBKV, we ought to cache the
// sockDrawer before we JSONify. We will do this when/if we add
// template based transformation of content.
func codecGPBJSONifyDataGPBKV(m *dataMsgGPB, s *msgToSerialise) {

	skipKeys := 0
	skipContent := 0
	empty := 0
	for _, topfield := range m.cachedDecode.GetDataGpbkv() {
		//
		// Extract timestamp at this level
		var rts rowToSerialise
		// At this level we may have a populated timestamp and fields
		// leading to key/value
		rts.Timestamp = topfield.Timestamp
		for _, kcfield := range topfield.GetFields() {
			// We should be iterating over two fields at this level;
			// keys and content.
			//
			fields := kcfield.GetFields()
			if len(fields) > 0 {
				if kcfield.Name == "keys" {
					sKeys, err := json.Marshal(telemetryKvToSockDrawer(fields))
					if err == nil {
						keys := json.RawMessage(sKeys)
						rts.Keys = &keys
					} else {
						skipKeys++
					}
				} else if kcfield.Name == "content" {
					sContent, err := json.Marshal(telemetryKvToSockDrawer(fields))
					if err == nil {
						content := json.RawMessage(sContent)
						rts.Content = &content
					} else {
						skipContent++
					}
				} else {
					empty++
				}
			}
		}
		s.Rows = append(s.Rows, &rts)
	}

	if skipKeys > 0 || skipContent > 0 {
		codecMetaMonitor.BasePathDecodeError.WithLabelValues(
			GPBCODEC, m.source.String(), m.cachedDecode.EncodingPath,
			"partial decode").Inc()
	}
}

//
// Produce byte stream from compact GPB encoded content.
func codecGPBJSONifyDataGPB(
	m *dataMsgGPB, s *msgToSerialise) {

	skipKeys := 0
	skipContent := 0

	marshaller := &jsonpb.Marshaler{
		//
		// EmitUInt64Unquoted ensures that gpb int64/uint64 fields are
		// marshalled unstringified.
		//
		// jsonpb marshals int64,uint64 to string by default.
		// https://github.com/golang/protobuf/issues/211
		// https://tools.ietf.org/html/rfc7159#section-6
		// http://stackoverflow.com/questions/16946306/preserve-int64-values-when-parsing-json-in-go
		//
		// Paraphrased: while controversial, it is deemed safer to use
		// string encoding for u/int64 to make sure that
		// implementations using IEEE574 for numbers do not go wrong
		// on numbers outside the 53 bits of integer precision they
		// support. Hence their choice for 64 bit being a string in
		// the mapping.
		//
		// While we control consumers (e.g. no js consumers), we will
		// marshal to numbers, so unmarshalling on the other side can
		// be results in a comparable numeric without special case
		// coercion.
		//
		// If compilation fails because EmitUInt64Unquoted is not an
		// attribute of jsonpb.Marshaller, it probably means that the
		// vendored protobuf package was updated and 'go generate' was
		// not rerun to patch in vendor.patch.
		//
		EmitUInt64Unquoted: true,
		EmitDefaults:       true,
		OrigName:           true,
	}

	//
	// Fetch of decode and cache deep decode of GPB content.
	tbl := m.getGPBDecodedTbl()
	if tbl == nil {

		// No mapping available... just track base64 encoded hex.

		compactGPBTable := m.cachedDecode.GetDataGpb()
		if compactGPBTable == nil {
			return
		}
		rows := compactGPBTable.GetRow()
		if rows == nil {
			return
		}

		for _, row := range rows {

			var rts rowToSerialise
			rts.Timestamp = row.Timestamp

			if len(row.Keys) > 0 {
				decodedKeysJSON, err := json.Marshal(
					map[string]string{
						"hexdump": base64.StdEncoding.EncodeToString(row.Keys)})
				if err == nil {
					keys := json.RawMessage(decodedKeysJSON)
					rts.Keys = &keys
				} else {
					skipKeys++
				}
			}

			if len(row.Content) > 0 {
				decodedContentJSON, err := json.Marshal(
					map[string]string{
						"hexdump": base64.StdEncoding.EncodeToString(row.Content)})
				if err == nil {
					content := json.RawMessage(decodedContentJSON)
					rts.Content = &content
				} else {
					skipContent++
				}
			}

			s.Rows = append(s.Rows, &rts)
		}

	} else {

		for _, row := range tbl.rows {

			var rts rowToSerialise
			rts.Timestamp = row.Timestamp

			decodedContentJSON, err := marshaller.MarshalToString(row.Content)
			if err != nil {
				skipContent++
			} else {
				content := json.RawMessage(decodedContentJSON)
				rts.Content = &content
			}

			decodedKeysJSON, err := marshaller.MarshalToString(row.Keys)
			if err != nil {
				skipKeys++
			} else {
				keys := json.RawMessage(decodedKeysJSON)
				rts.Keys = &keys
			}

			s.Rows = append(s.Rows, &rts)
		}
	}

	if skipKeys > 0 || skipContent > 0 {
		codecMetaMonitor.BasePathDecodeError.WithLabelValues(
			GPBCODEC, m.source.String(), m.cachedDecode.EncodingPath,
			"partial decode").Inc()
	}
}

//
// This function is capable of producing streams for GPB (passed
//through from input), JSON and JSON events from GPB. GPB in this
//context means GPB K/V or compact encoded using the common header.
func (m *dataMsgGPB) produceByteStream(
	streamSpec *dataMsgStreamSpec) (error, []byte) {

	switch streamSpec.streamType {

	case dMStreamGPB, dMStreamMsgDefault:
		// Simply return the original encoded message
		return nil, m.original

	case dMStreamTemplate:
		var b bytes.Buffer
		var msg msgToFilter

		msg.populateDataFromGPB(m.cachedDecode)
		if streamSpec.context != nil {
			parsedTemplate := streamSpec.context.(*template.Template)
			err := parsedTemplate.Execute(&b, msg)
			return err, b.Bytes()
		}

		return fmt.Errorf("GPB CODEC: parsed template missing"), nil

	case dMStreamJSONEvents, dMStreamJSON:

		var copy telem.Telemetry

		marshaller := &jsonpb.Marshaler{
			// See long comments above EmitUInt64Unquoted, eslewhere
			// in this file.
			EmitUInt64Unquoted: true,
			EmitDefaults:       true,
			OrigName:           true}

		var s msgToSerialise
		copy = *m.cachedDecode

		// Remarshal the decoded content to JSON
		compactGPBTable := m.cachedDecode.GetDataGpb()
		if compactGPBTable != nil {
			// If we have compact GPB table with rows, we need to decode
			// further.
			codecGPBJSONifyDataGPB(m, &s)
			copy.DataGpb = nil
		} else {
			//
			// We need to unk/v the content before we JSONify.
			codecGPBJSONifyDataGPBKV(m, &s)
			copy.DataGpbkv = nil
		}

		telemetryJSON, err := marshaller.MarshalToString(&copy)
		if err != nil {
			return err, nil
		}
		telemetryJSONRaw := json.RawMessage(telemetryJSON)
		s.Telemetry = &telemetryJSONRaw

		//
		// Track the source as picked off the wire.
		s.Source = m.source.String()

		//
		// Finally serialise
		if streamSpec.streamType == dMStreamJSONEvents {
			if len(s.Rows) == 0 {
				//
				// Nothing to generate. This typically happens when a
				// message is received with collection end time and
				// nothing else.
				return nil, nil
			}
			//
			// In this case we are producing a JSON array of events, where
			// each event carries header information with it. This makes consumption
			// in third party consumers easier in some cases.
			var buffer bytes.Buffer
			encoder := json.NewEncoder(&buffer)
			buffer.WriteString("[")
			first := true

			type msgToSerialise2 struct {
				Source    string
				Telemetry *json.RawMessage
				Row       *rowToSerialise `json:"Rows,omitempty"`
			}

			var r msgToSerialise2
			r.Source = s.Source
			r.Telemetry = s.Telemetry
			for _, row := range s.Rows {
				if first {
					first = false
				} else {
					buffer.WriteString(",")
				}
				r.Row = row
				err := encoder.Encode(r)
				if err != nil {
					return fmt.Errorf("Marshalling collected event content, [%+v][%+v]",
						r, err), nil
				}
			}
			buffer.WriteString("]")
			return nil, buffer.Bytes()

		} else {
			//
			// Serialise the whole batch as it is produced.
			b, err := json.Marshal(s)
			if err != nil {
				return fmt.Errorf("Marshalling collected content, [%+v][%+v]",
					s, err), nil
			}

			return err, b
		}
	}

	return fmt.Errorf("GPB CODEC: reformat GPB msg to [%s] is"+
		" not supported", dataMsgStreamTypeString(streamSpec.streamType)), nil
}

func (m *dataMsgGPB) produceGPBKVMetricsForNode(
	spec *metricsSpec,
	node *metricsSpecNode,
	fields []*telem.TelemetryField,
	timestamp uint64,
	tags []metricsAtom,
	outputHandler metricsOutputHandler,
	buf metricsOutputContext) {

	var ts uint64
	var val interface{}
	var written bool

	//
	// We run multiple times through a given level.
	//  - first pass we extract tags,
	//  - second pass we extract sensors,
	//  - third pass we recurse down into children.
	//
	// We exploit the fact that at least at the top level, tags are
	// highly likely to be at the beginning.
	//
	// Alternative would have required us to collect all sensors in
	// the pass before writing them out to make sure that tags are all
	// present.
	//
	fieldsMap := node.fieldsMapsByType[metricsSpecNodeTypeTag]
	collected := 0
	tagsTarget := len(fieldsMap)
	if tagsTarget > 0 {

		for _, field := range fields {

			child, ok := fieldsMap[field.Name]
			if !ok {
				continue
			}

			val = extractGPBKVNativeTypeFromOneof(field, false)
			if val != nil {
				tags = append(tags, metricsAtom{
					key: child.fqName,
					val: val,
				})
			}

			collected++
			if collected >= tagsTarget {
				break
			}
		}
	}

	fieldsMap = node.fieldsMapsByType[metricsSpecNodeTypeSensor]
	sensorTarget := len(fieldsMap)
	if sensorTarget != 0 {
		for _, field := range fields {

			child, ok := fieldsMap[field.Name]
			if !ok {
				continue
			}

			//
			// Choose timestamp to pass to writer
			if field.Timestamp == 0 {
				ts = timestamp
			} else {
				ts = field.Timestamp
			}

			if child.Track {

				buf := new(bytes.Buffer)
				buf.WriteString(child.fqName)
				for i := 0; i < len(tags); i++ {
					buf.WriteString(
						fmt.Sprintf(
							" %s=\"%v\"",
							tags[i].key,
							tags[i].val))
				}
				//
				// We're tracking stats for this one...
				spec.stats.statRecordUpdate(buf.String(), ts)
			}

			val = extractGPBKVNativeTypeFromOneof(field, false)
			if val != nil {
				outputHandler.buildMetric(
					tags,
					metricsAtom{
						key: child.fqName,
						val: val,
					},
					ts,
					buf)
				written = true
			}
			//
			// We cannot break early here in order to be able to
			// support leaf lists. i.e. multiple repeated instances of
			// the same name
		}
	}

	if written {
		outputHandler.flushMetric(tags, ts, buf)
	}

	fieldsMap = node.fieldsMapsByType[metricsSpecNodeTypeContainer]
	containerTarget := len(fieldsMap)
	if containerTarget != 0 {
		for _, field := range fields {

			child, ok := fieldsMap[field.Name]
			if !ok {
				continue
			}

			//
			// Choose the more precise timestamp to carry down
			if field.Timestamp == 0 {
				ts = timestamp
			} else {
				ts = field.Timestamp
			}

			m.produceGPBKVMetricsForNode(
				spec,
				child,
				field.GetFields(),
				ts,
				tags,
				outputHandler,
				buf)

			//
			// We cannot break early here since the collection of
			// fields may well be of the same type, like when we are
			// mapping a yang list.
		}
	}
}

func (m *dataMsgGPB) produceGPBKVMetrics(
	spec *metricsSpec,
	node *metricsSpecNode,
	outputHandler metricsOutputHandler,
	tags []metricsAtom,
	buf metricsOutputContext) error {

	var timestamp uint64

	for _, topField := range m.cachedDecode.GetDataGpbkv() {

		if topField.Timestamp != 0 {
			timestamp = topField.Timestamp
		} else {
			timestamp = m.cachedDecode.MsgTimestamp
		}

		tagsCopy := tags
		for _, kcfield := range topField.GetFields() {
			//
			// We should be iterating over two fields at this level;
			// keys and content. Automatically add keys to tags. We
			// rely on the order, keys first, than content. We could
			// make this more robust and break dependency.
			if kcfield.Name == "keys" {
				for _, key := range kcfield.GetFields() {
					tagsCopy = append(tagsCopy, metricsAtom{
						key: outputHandler.adaptTagName(key.Name),
						val: extractGPBKVNativeTypeFromOneof(key, false),
					})
				}
			}

			if kcfield.Name == "content" {
				m.produceGPBKVMetricsForNode(
					spec,
					node,
					kcfield.GetFields(),
					timestamp,
					tagsCopy,
					outputHandler,
					buf)
			}
		}
	}

	return nil
}

//
// getGPBDecodedTbl returns the cached decoded table. If the content
// has not been decoded yet, content is decoded and cached too.
func (m *dataMsgGPB) getGPBDecodedTbl() *gpbDecodeTbl {

	tblVal := m.cachedDecodeTbl.Load()
	if tblVal == nil {

		//
		// if shallow decode is missing, nothing doing
		compactGPBTable := m.cachedDecode.GetDataGpb()
		if compactGPBTable == nil {
			//
			// Legit e.g. when sending just the header with end of
			// collection.
			return nil
		}

		//
		// We will build and then set the cached value. Note that,
		// while we're decoding, others might and we may end up
		// replacing the cached value.
		m.cachedDecodeTblLock.Lock()
		defer m.cachedDecodeTblLock.Unlock()

		tblVal = m.cachedDecodeTbl.Load()
		if tblVal == nil {
			mapping := telem.EncodingPathToMessageReflectionSet(
				&telem.ProtoKey{
					EncodingPath: m.cachedDecode.EncodingPath,
					Version:      ""})

			if mapping == nil {
				codecMetaMonitor.BasePathDecodeError.WithLabelValues(
					GPBCODEC, m.source.String(), m.cachedDecode.EncodingPath,
					"proto archive does not support path/version").Inc()
				return nil
			}

			skipKeys := 0
			skipContent := 0
			rows := compactGPBTable.GetRow()
			cachedTbl := &gpbDecodeTbl{
				rows: make([]*gpbDecodeRow, 0, len(rows)),
			}
			for _, row := range rows {

				var decodedKeysMsg proto.Message
				var decodedContentMsg proto.Message

				srowContentType := mapping.MessageReflection(
					telem.PROTO_CONTENT_MSG)
				contentType := srowContentType.Elem()
				decodedContent := reflect.New(contentType)
				decodedContentMsg =
					decodedContent.Interface().(proto.Message)
				err := proto.Unmarshal(row.Content, decodedContentMsg)
				if err != nil {
					skipContent++
				}

				srowKeysType := mapping.MessageReflection(
					telem.PROTO_KEYS_MSG)
				if srowKeysType != nil {
					keysType := srowKeysType.Elem()
					decodedKeys := reflect.New(keysType)
					decodedKeysMsg =
						decodedKeys.Interface().(proto.Message)
					err = proto.Unmarshal(row.Keys, decodedKeysMsg)
					if err != nil {
						skipKeys++
					}
				}

				cachedTbl.rows = append(cachedTbl.rows, &gpbDecodeRow{
					Timestamp: row.Timestamp,
					Keys:      decodedKeysMsg,
					Content:   decodedContentMsg,
				})
			}
			//
			// Cache value
			m.cachedDecodeTbl.Store(cachedTbl)

			if skipKeys > 0 || skipContent > 0 {
				codecMetaMonitor.BasePathDecodeError.WithLabelValues(
					GPBCODEC, m.source.String(), m.cachedDecode.EncodingPath,
					"partial decode").Inc()
			}

			//
			// Reload cached content
			tblVal = m.cachedDecodeTbl.Load()
		}
	}

	if tblVal != nil {
		return tblVal.(*gpbDecodeTbl)
	}

	return nil
}

// Compile regex for extracting field names from protobuf tag struct
// just the once. A Regexp is safe for concurrent use by multiple
// goroutines.
var codecGPBFieldNameParser = regexp.MustCompile("name=(.*?)(,json|$)")
var codecGPBFieldNameGroup = 1

//
// codecGPBExtractFieldName takes a field value from a type, and
// extracts name from protobuf tag.
func codecGPBExtractFieldName(ft reflect.StructField) string {

	pbt, ok := ft.Tag.Lookup("protobuf")
	if !ok {
		return ft.Name
	}

	matchgroup := codecGPBFieldNameParser.FindStringSubmatch(pbt)
	if matchgroup == nil {
		return ft.Name
	}

	if len(matchgroup) < codecGPBFieldNameGroup+1 {
		return ft.Name
	}

	return matchgroup[codecGPBFieldNameGroup]
}

func (m *dataMsgGPB) produceGPBMetricsForNode(
	spec *metricsSpec,
	node *metricsSpecNode,
	refv reflect.Value,
	timestamp uint64,
	tags []metricsAtom,
	outputHandler metricsOutputHandler,
	buf metricsOutputContext) {

	var written bool

	if refv.Kind() != reflect.Struct {
		return
	}

	intNamesCached := node.internalNamesCached[dMStreamGPB].Load().(bool)
	if !intNamesCached {
		node.internalNamesCachedLock.Lock()
		//
		// Check if cache has been loaded between when we checked and
		// when we locked. If it has we're done.
		intNamesCached = node.internalNamesCached[dMStreamGPB].Load().(bool)
		if !intNamesCached {
			reftyp := refv.Type()
			for i := 0; i < refv.NumField(); i++ {
				fldtype := reftyp.Field(i)
				extName := codecGPBExtractFieldName(fldtype)
				childNode, ok := node.fieldsMap[extName]
				if ok {
					childNode.internalName[dMStreamGPB] = fldtype.Name
				}
			}
			node.internalNamesCached[dMStreamGPB].Store(true)
		}
		node.internalNamesCachedLock.Unlock()
	}

	//
	// Do tag leaves at this level. Because the json name is not the
	// same as protobuf name, spec must account for this. This will
	// need to be fixed by fixing up the mapping type when building
	// the spec.
	fieldsMap := node.fieldsMapsByType[metricsSpecNodeTypeTag]
	for fieldName, node := range fieldsMap {
		//
		// We only ever get here when internal names have been cached,
		// and once cached, internal names are never updated so we
		// don't need to take a lock.
		intName := node.internalName[dMStreamGPB]
		if intName == "" {
			intName = fieldName
		}
		fval := refv.FieldByName(intName)
		if !fval.IsValid() {
			//
			// Spec mismatch for a field?
			countErr := fmt.Sprintf("metric extract tag %s %s", node.fqName,
				fieldName)
			codecMetaMonitor.BasePathDecodeError.WithLabelValues(
				GPBCODEC, m.source.String(), m.cachedDecode.EncodingPath,
				countErr).Inc()
			continue
		}
		tagname := outputHandler.adaptTagName(fieldName)
		tags = append(tags, metricsAtom{
			key: tagname, val: fval,
		})
	}

	//
	// Do sensors at this level. Only K/V supports "track". Should be
	// easy to add if necessary.
	fieldsMap = node.fieldsMapsByType[metricsSpecNodeTypeSensor]
	for sensorName, sensorNode := range fieldsMap {

		intName := sensorNode.internalName[dMStreamGPB]
		if intName == "" {
			intName = sensorName
		}
		fval := refv.FieldByName(intName)
		if !fval.IsValid() {
			//
			// Spec mismatch for a field?
			countErr := fmt.Sprintf("metric extract sensor %s %s",
				node.fqName, sensorName)
			codecMetaMonitor.BasePathDecodeError.WithLabelValues(
				GPBCODEC, m.source.String(), m.cachedDecode.EncodingPath,
				countErr).Inc()
			continue
		}
		outputHandler.buildMetric(
			tags,
			metricsAtom{
				key: sensorNode.fqName,
				val: fval,
			},
			timestamp,
			buf)
		written = true
	}

	if written {
		outputHandler.flushMetric(tags, timestamp, buf)
	}

	//
	// Time to recurse down for containers.
	fieldsMap = node.fieldsMapsByType[metricsSpecNodeTypeContainer]
	for containerName, containerNode := range fieldsMap {

		intName := containerNode.internalName[dMStreamGPB]
		if intName == "" {
			intName = containerName
		}
		fval := refv.FieldByName(intName)
		if !fval.IsValid() {
			//
			// Spec mismatch for a container name?
			countErr := fmt.Sprintf("metric extract container %s %s",
				node.fqName, containerName)
			codecMetaMonitor.BasePathDecodeError.WithLabelValues(
				GPBCODEC, m.source.String(), m.cachedDecode.EncodingPath,
				countErr).Inc()
			continue
		}
		switch fval.Kind() {
		case reflect.Ptr, reflect.Struct:
			m.produceGPBMetricsForNode(
				spec,
				containerNode,
				reflect.Indirect(fval),
				timestamp,
				tags,
				outputHandler,
				buf)
		case reflect.Slice:
			for i := 0; i < fval.Len(); i++ {
				//
				// Each of the member of the slice should have at
				// least one tag with unique value across slice to
				// disambiguate. Otherwise only the last one will be
				// retained.
				m.produceGPBMetricsForNode(
					spec,
					containerNode,
					reflect.Indirect(fval.Index(i)),
					timestamp,
					tags,
					outputHandler,
					buf)
			}
		}
	}
}

func (m *dataMsgGPB) produceGPBMetrics(
	spec *metricsSpec,
	node *metricsSpecNode,
	parent_timestamp uint64,
	outputHandler metricsOutputHandler,
	tags []metricsAtom,
	buf metricsOutputContext) error {

	//
	// Get cached decode (or decode if not cached). If absent decode
	// counter errors are incremented.
	tbl := m.getGPBDecodedTbl()
	if tbl == nil {
		return nil
	}

	for _, row := range tbl.rows {
		var timestamp uint64

		if row.Timestamp != 0 {
			timestamp = row.Timestamp
		} else {
			timestamp = parent_timestamp
		}

		// Take a copy of the tags
		tagsCopy := tags

		//
		// Extract keys and add them to tags first.
		if row.Keys != nil {
			rval := reflect.Indirect(reflect.ValueOf(row.Keys))
			rtyp := rval.Type()
			for i := 0; i < rval.NumField(); i++ {
				fld := rval.Field(i)
				fldType := rtyp.Field(i)
				tagname := outputHandler.adaptTagName(
					codecGPBExtractFieldName(fldType))
				tagsCopy = append(tagsCopy, metricsAtom{
					key: tagname, val: fld,
				})
			}
		}

		//
		// Navigate spec to extract content.
		if row.Content != nil {
			m.produceGPBMetricsForNode(
				spec,
				node,
				reflect.Indirect(reflect.ValueOf(row.Content)),
				timestamp,
				tagsCopy,
				outputHandler,
				buf)
		}
	}

	return nil
}

func (m *dataMsgGPB) produceMetrics(
	spec *metricsSpec,
	outputHandler metricsOutputHandler,
	buf metricsOutputContext) error {

	//
	// Find corresponding metric spec node. If not, bail out fast - no
	// metrics from this message.
	telemMsg := m.cachedDecode
	if telemMsg == nil {
		return nil
	}

	node, ok := spec.specDB[telemMsg.EncodingPath]
	if !ok {
		return nil
	}

	//
	// Start with slice of capacity 8. This should cover most cases,
	// but we use append anyway which will grow the slice if necessary
	// Size should reflect the fixed tags we add when we kick off.
	tags := make([]metricsAtom, 2, 8)

	tags[0].key = "EncodingPath"
	tags[0].val = telemMsg.EncodingPath
	tags[1].key = "Producer"
	tags[1].val = telemMsg.GetNodeIdStr()
	if tags[1].val == "" {
		tags[1].val = m.source.String()
	}

	// Handle compact GPB, if GPB encoded.
	if telemMsg.GetDataGpb() != nil {
		return m.produceGPBMetrics(
			spec,
			node,
			telemMsg.MsgTimestamp,
			outputHandler,
			tags,
			buf)
	}

	// Handle GPBKV, if GPBKV encoded.
	if telemMsg.GetDataGpbkv() != nil {
		return m.produceGPBKVMetrics(
			spec,
			node,
			outputHandler,
			tags,
			buf)
	}

	return nil

}

func codecGPBBlockAndTelemMsgToDataMsgs(
	p *codecGPB, source msgproducer, nextBlock []byte, telem *telem.Telemetry) (
	error, []dataMsg) {

	m := &dataMsgGPB{
		cachedDecode: telem,
		original:     nextBlock,
		source:       source,
	}

	dMs := make([]dataMsg, 1)
	dMs[0] = m

	// Count the extracted message against the source, section and
	// and base path (untranslated).
	err, base_path := m.getMetaDataPath()
	if err == nil {
		codecMetaMonitor.BasePathGroups.WithLabelValues(
			p.name, source.String(), base_path).Inc()
	}

	//
	// Determine whether we need to translate encoding path. Note that
	// we can receive old MDA paths in both old PDT format and new MDT
	// format (backported MDT without YANG support). Hence the
	// lookaside at this common point.
	if basePathXlation != nil && base_path != "" {
		newPath, ok := basePathXlation[base_path]
		if ok {
			m.cachedDecode.EncodingPath = newPath
		}
	}

	return nil, dMs
}

//
// Handle the unified .proto which is capable of carrying both K/V and
// compact GPB content.
func codecGPBBlockToDataMsgs(
	p *codecGPB, source msgproducer, nextBlock []byte) (error, []dataMsg) {

	telem := &telem.Telemetry{}
	err := proto.Unmarshal(nextBlock, telem)

	if err != nil {
		//
		// We failed to extract gpb. Drop.
		return err, nil
	}

	return codecGPBBlockAndTelemMsgToDataMsgs(p, source, nextBlock, telem)
}

//
// Extract DM conforming to latest .proto from old style PDT messages.
func codecGPBLegacyPDTBlockToDataMsgs(
	p *codecGPB, source msgproducer, nextBlock []byte) (error, []dataMsg) {

	target := &telem.Telemetry{}
	old := &pdt.TelemetryHeader{}
	err := proto.Unmarshal(nextBlock, old)
	if err != nil {
		//
		// We failed to extract gpb. Drop.
		return err, nil
	}

	target.NodeId = &telem.Telemetry_NodeIdStr{NodeIdStr: old.Identifier}
	target.Subscription = &telem.Telemetry_SubscriptionIdStr{SubscriptionIdStr: old.PolicyName}
	target.CollectionStartTime = old.StartTime
	target.MsgTimestamp = old.StartTime
	target.CollectionEndTime = old.EndTime

	targetTable := &telem.TelemetryGPBTable{
		Row: make([]*telem.TelemetryRowGPB, 0, CODEC_GPB_PREALLOC_PDT_ROWS)}
	target.DataGpb = targetTable

	for _, table := range old.GetTables() {

		if target.EncodingPath == "" {
			target.EncodingPath = table.PolicyPath
		} else {
			if 0 != strings.Compare(target.EncodingPath, table.PolicyPath) {
				//
				// We expect path to be the same in every message!!
				return fmt.Errorf(
					"different paths encoded in the same message (unsupported): %s, %s",
					target.EncodingPath, table.PolicyPath), nil
			}
		}

		for _, row := range table.GetRow() {
			//
			// Keys are not separated out, and timestamp is not set by
			// default, and even if it were, it is embedded in the
			// message itself. No keys, no timestamp then?
			//
			// Not so fast. For keys we use exploit a property of
			// protobuf encoding. Inefficient, but..., here is what we
			// do.  We associate the full encoded content with both
			// Keys and Content. This will result in us trying to
			// decode the message against the keys and the content
			// independently. Because the field numbers do NOT
			// overlap, it just works, at the rereading the message
			// (for keys we skip over most of it anyway).
			target.DataGpb.Row = append(target.DataGpb.Row,
				&telem.TelemetryRowGPB{
					Keys:    row,
					Content: row, // No! Not a mistake. Read above.
				})
		}
	}

	if len(target.DataGpb.Row) == 0 {
		return fmt.Errorf("no content in PDT message: %s", target.EncodingPath), nil
	}

	//
	// We need to remarshal target, so we keep the binary, not as received,
	// but as the new version of telemetry.
	newContent, err := proto.Marshal(target)
	if err != nil {
		return fmt.Errorf("failed to remarshall to new format: %v", err), nil
	}

	return codecGPBBlockAndTelemMsgToDataMsgs(p, source, newContent, target)
}

// nextBlock allows GPB to produce dataMsg set from the current binary
// stream.  Note that GPB parser only supports nextBlock, and not
// nextBlockBuffer.
func (p *codecGPB) blockToDataMsgs(source msgproducer, nextBlock []byte) (
	error, []dataMsg) {

	//
	// Count the decoded message against the source, section and
	// type
	codecMetaMonitor.Decoded.WithLabelValues(
		p.name, source.String(), encodingToName(p.gpbType)).Inc()
	codecMetaMonitor.DecodedBytes.WithLabelValues(
		p.name, source.String(), encodingToName(p.gpbType)).Add(
		float64(len(nextBlock)))

	switch p.gpbType {

	case ENCODING_GPB:
		return codecGPBBlockToDataMsgs(p, source, nextBlock)

	case ENCODING_GPB_COMPACT:
		return codecGPBLegacyPDTBlockToDataMsgs(p, source, nextBlock)

	case ENCODING_GPB_KV:
		//
		// Legacy handling of PDT, and prerelease MDT format
		break

	}

	return fmt.Errorf("ENCAP GPB: Product type for GPB parser not set"), nil
}

type codecGPB struct {
	name    string
	gpbType encoding
}

//
// Produce a GPB type codec
func getNewCodecGPB(name string, e encoding) (error, codec) {
	c := &codecGPB{
		name:    name,
		gpbType: e,
	}

	return nil, c
}
