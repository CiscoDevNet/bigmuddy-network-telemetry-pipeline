//
// June 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Extract metrics from telemetry data. This module is independent of
// the specific metrics package (e.g. prometheus, opentsdb etc). The
// specific metrics package handling can be found in metrics_x.go.
//
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type metricsOutputType int
type metricsOutputContext interface{}

//
// Defaults
const (
	//
	// Period when we look for stale stat records to evict
	STATEVICTIONPERIOD = 1 * time.Minute
	//
	// Stat for sensor which is not updated for 5 minutes is dropped
	// as stale making way for others.
	STATEVICTIONSTALE = 300000
)

const (
	metricsTypePrometheus = iota
	metricsTypeInflux
	metricsTypeTest
)

var metricsTypeMap = map[string]metricsOutputType{
	"prometheus": metricsTypePrometheus,
	"influx":     metricsTypeInflux,
	"test":       metricsTypeTest,
}

type metricsOutputHandler interface {
	setupWorkers(module *metricsOutputModule)
	buildMetric(tags []metricsAtom, sensor metricsAtom, ts uint64,
		context metricsOutputContext)
	flushMetric(tags []metricsAtom, ts uint64, context metricsOutputContext)
	adaptSensorName(name string) string
	adaptTagName(name string) string
}

var testOutputHandler metricsOutputHandler

//
// Structures used to collect metrics and tags.
type metricsAtom struct {
	key string
	val interface{}
}

//
// The metricsSpecForBasepath is made of metricSpecNode objects which
// are themselves organised as a hierarchy and together they describe
// the metrics extracted from a message associated with a basepath.
//
// The global metricSpec represents a collection of
// metricsSpecForBasepath objects, and describes the set of metrics
// which are extracted from the streams.
//
// The metrics module is expected to take dMs in (message atoms
// produced by the input side), and produces:
//
// sensor_name {key1=value1, key2=value2, ...} value timestamp
//
type metricsSpecNodeType int

const (
	metricsSpecNodeTypeSensor = iota
	metricsSpecNodeTypeTag
	metricsSpecNodeTypeContainer
	// Last value for sizing array
	metricsSpecNodeTypeARRAYSIZE
)

type metricsSpecNode struct {
	//
	// Content directly from the spec.
	//
	//  - Name is the sensor or tag field name
	//  - Tag indicates that field is tag field name if set to true
	//  - Track indicate that we are exporting delta between
	//    collections as part of metamonitoring.
	//  - Fields provides for recursion
	//
	Name   string
	Tag    bool
	Track  bool
	Fields []*metricsSpecNode
	//
	// Derived content to support efficient generation of metrics
	//
	// Track derived nodetype to support efficient traversal
	nodeType metricsSpecNodeType
	//
	// If we have fields beneath this node, than we have a map
	// which allows us to find nodes quickly. We also track
	// composition.
	fieldsMap        map[string]*metricsSpecNode
	fieldsMapsByType [metricsSpecNodeTypeARRAYSIZE]map[string]*metricsSpecNode
	//
	// For sensor and tag nodes, track the sensor name which will be
	// the path down the hierarchy (for disambiguation).
	fqName string
	//
	// Internal names of children nodes have been cached per
	// dataMsgStreamType. Value is loaded/stored atomically
	//
	// list is indexed by dMStreamType.
	internalNamesCached     []atomic.Value
	internalNamesCachedLock sync.Mutex
	//
	// Map, per encoding, the name specified externally and how it
	// maps to the internal name. e.g. for compact gpb, we go from
	// process-cpu-fifteen-minute to ProcessCpuFifteenMinute;
	// ProcessCpuFifteenMinute uint32
	// `protobuf:"varint,5,opt,name=process_cpu_fifteen_minute,json=processCpuFifteenMinute"
	// json:"process_cpu_fifteen_minute,omitempty"` Note that we
	// populate this field the first time we use it. We could improve
	// this by doing it when we create the spec.
	//
	// list is indexed by dMStreamType.
	internalName []string
}

type metricsSpecForBasepath struct {
	// Base path
	Basepath string
	// Root specNode at this basepath
	Spec *metricsSpecNode
}

//
// Stat collection
type statRecord struct {
	sync.Mutex
	lastTS  uint64
	deltaTS prometheus.Summary
}

type statRecordTable struct {
	sync.Mutex
	statRecords map[string]*statRecord
	//
	// If we are collecting statistics, we will have specs here.
	//
	// - Number of sensors stated
	//
	statSensorCount int
	//
	// module name
	name string
	//
	// Channel used for eviction timer
	evict chan bool
}

func (rt *statRecordTable) statRecordFindOrCreate(
	key string, ts uint64) *statRecord {

	rt.Lock()
	defer rt.Unlock()

	r, ok := rt.statRecords[key]
	if !ok {

		if len(rt.statRecords) >= rt.statSensorCount {
			// We are oversubscribed, no space. We won't sample this
			// one until some stale ones are evicted.
			return nil
		}

		r = &statRecord{
			lastTS:  ts,
			deltaTS: metricsStatMonitor.deltaTS.WithLabelValues(key),
		}

		rt.statRecords[key] = r

	}

	return r
}

func (rt *statRecordTable) statRecordUpdate(
	key string,
	ts uint64) {

	r := rt.statRecordFindOrCreate(key, ts)
	if r == nil {
		metricsStatMonitor.statCacheFull.WithLabelValues(key).Inc()
		return
	}

	//
	// We have one of these already. We need to compute delta from previous and
	// reigster observation, and update to new ts
	r.Lock()
	defer r.Unlock()

	// logger.WithFields(
	//	log.Fields{
	//		"name":  rt.name,
	//		"key":   key,
	//		"newts": ts,
	//		"oldts": r.lastTS,
	//		"delta": float64(ts - r.lastTS),
	//	}).Debug("stat tracking record update")

	if ts > r.lastTS {
		r.deltaTS.Observe(float64(ts - r.lastTS))
		r.lastTS = ts
	} else if ts < r.lastTS {
		//
		// Protest loudly. No time travel allowed.
		metricsStatMonitor.outOfSeq.WithLabelValues(key).Inc()
	}
}

func (rt *statRecordTable) scheduleEviction() {
	time.Sleep(STATEVICTIONPERIOD)
	rt.evict <- true
}

func (rt *statRecordTable) evictStale() {

	ts := time.Now().Unix() * 1000
	count := 0

	rt.Lock()
	defer rt.Unlock()

	for key, r := range rt.statRecords {
		r.Lock()
		if uint64(ts) > r.lastTS && uint64(ts)-r.lastTS > STATEVICTIONSTALE {
			delete(rt.statRecords, key)
			count++
		}
		r.Unlock()
	}

	if count > 0 {
		logger.WithFields(
			log.Fields{
				"name":          rt.name,
				"evicted count": count,
				"occupied":      len(rt.statRecords),
				"max":           rt.statSensorCount,
			}).Debug("stats tracking evicted statRecord entries from cache")
	}
}

type metricsSpec struct {
	specSet []metricsSpecForBasepath
	//
	// Lookup support mapping input to metrics derived
	// from metrics specification
	specDB map[string]*metricsSpecNode
	//
	// Statistics records tracked against his spec.
	stats *statRecordTable
}

type metricsOutputModule struct {
	//
	// Module section name
	name string
	//
	// Metrics specification provided by operator, and derived
	// structures used in the datapath
	inputSpecFile string
	inputSpec     metricsSpec
	//
	// Output handler shipping the metrics out
	output        string
	outputHandler metricsOutputHandler
	//
	// Interaction with producers and orchestrator
	dataChannelDepth int
	ctrlChan         chan *ctrlMsg
	dataChan         chan dataMsg
	// Interaction with output handlers. When control loop learns it
	// is time to leave, shutdownChan is closed, and wait on wait
	// group. This allows us to exit clean.
	shutdownSyncPoint sync.WaitGroup
	shutdownChan      chan struct{}
}

func metricsOutputModuleNew() outputNodeModule {
	return &metricsOutputModule{}
}

func (m *metricsOutputModule) buildSpecDBSubtree(
	node *metricsSpecNode, path string, conjoinsymbol string) error {

	if node.Name != "" {
		if path != "" {
			path = path + conjoinsymbol + node.Name
		} else {
			path = node.Name
		}
	}

	logCtx := logger.WithFields(log.Fields{
		"name": m.name,
		"path": path,
	})

	//
	// Initialise internal name translation
	// machinery. internalNamesCached is set up to indicate that child
	// nodes name mapping per stream type is uncached. Set internal
	// node name to empty too.
	node.internalNamesCached = make([]atomic.Value, dMStreamMsgDefault)
	for i := 0; i < int(dMStreamMsgDefault); i++ {
		node.internalNamesCached[i].Store(false)
	}
	node.internalName = make([]string, dMStreamMsgDefault)

	//
	// Initialise maps tracking the various fields
	node.fieldsMap = make(map[string]*metricsSpecNode)
	for i := 0; i < metricsSpecNodeTypeARRAYSIZE; i++ {
		node.fieldsMapsByType[i] = make(map[string]*metricsSpecNode)
	}

	//
	// Validate and determine node type
	if node.Tag {
		if len(node.Fields) != 0 {
			err := fmt.Errorf(
				"Node %v is labelled 'Tag' but contains 'Fields'\n", node)
			logCtx.WithError(err).Error("Setting up metrics spec")
			return err
		}
		if node.Track {
			err := fmt.Errorf(
				"Node %v is labelled 'Tag' and 'Track' (track ignored)\n", node)
			logCtx.WithError(err).Error("Setting up metrics spec")
		}

		node.nodeType = metricsSpecNodeTypeTag
		node.fqName = m.outputHandler.adaptTagName(path)

	} else if len(node.Fields) != 0 {
		node.nodeType = metricsSpecNodeTypeContainer

		if node.Track {
			err := fmt.Errorf(
				"Node %v has children and is tagged 'Track' (track ignored)\n",
				node)
			logCtx.WithError(err).Error("Setting up metrics spec")
		}

		//
		// We must populate the fieldsMap such that as we
		// navigate, we can easily identify child nodes.
		for _, child := range node.Fields {

			node.fieldsMap[child.Name] = child
			err := m.buildSpecDBSubtree(child, path, conjoinsymbol)
			if err != nil {
				logCtx.WithError(err).Error("Setting up metrics spec")
				return err
			}
			node.fieldsMapsByType[child.nodeType][child.Name] = child

		}

	} else {
		node.nodeType = metricsSpecNodeTypeSensor
		node.fqName = m.outputHandler.adaptSensorName(path)
		if node.Track {
			logCtx.WithFields(log.Fields{
				"track": node.fqName,
			}).Debug("Setting up metrics spec")
		}
	}

	return nil
}

func (m *metricsOutputModule) buildSpecDB() error {

	m.inputSpec.specDB = make(map[string]*metricsSpecNode)

	for _, sbp := range m.inputSpec.specSet {

		//
		// Taverse the node hierarchy and build map per node level for
		// fast access to corresponding children nodes. Loop rather
		// than recurse. If we were to include the base path in the
		// metric name, we would replace "" with sbp.Basepath.
		err := m.buildSpecDBSubtree(sbp.Spec, "", "__")
		if err != nil {
			return err
		}
		//
		// Finally link the root node to the root of the lookup tree
		m.inputSpec.specDB[sbp.Basepath] = sbp.Spec

		logger.WithFields(log.Fields{
			"name":     m.name,
			"basepath": sbp.Basepath,
		}).Info("setup metrics collection")
	}

	return nil
}

func (m *metricsOutputModule) controlLoop() {

	var stats msgStats

	logCtx := logger.WithFields(log.Fields{
		"name":   m.name,
		"output": m.output,
		"file":   m.inputSpecFile,
	})

	//
	// Kick off data handlers and run control loop
	m.outputHandler.setupWorkers(m)

	for {
		select {
		case <-m.inputSpec.stats.evict:
			m.inputSpec.stats.evictStale()
			go m.inputSpec.stats.scheduleEviction()

		case msg := <-m.ctrlChan:
			switch msg.id {
			case REPORT:
				content, _ := json.Marshal(stats)
				resp := &ctrlMsg{
					id:       ACK,
					content:  content,
					respChan: nil,
				}
				msg.respChan <- resp

			case SHUTDOWN:
				logCtx.Info("metrics producer rxed SHUTDOWN")

				//
				// Signal any children that we are done.
				close(m.shutdownChan)
				m.shutdownSyncPoint.Wait()

				//
				// We're done pass it on. Would have been so nice to
				// use this wait group pattern trhoughout.
				resp := &ctrlMsg{
					id:       ACK,
					respChan: nil,
				}
				msg.respChan <- resp
				return

			default:
				logCtx.Error("metrics producer, unknown ctrl message")
			}
		}
	}
}

//
// Handle configuration for metrics
func (m *metricsOutputModule) configure(name string, nc nodeConfig) (
	error, chan<- dataMsg, chan<- *ctrlMsg) {

	var stat statRecordTable
	var err error

	//
	// Who am I?
	m.name = name

	//
	// Load configuration of what metrics to extract.
	m.inputSpecFile, err = nc.config.GetString(name, "file")
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{"name": name}).Error(
			"file option: metrics to collect required")
		return err, nil, nil
	}

	//
	// Parse input file in preparation for handling telemetry and to
	// indicate any input file errors at startup right here. Read it
	// in one go.
	specByteStream, err := ioutil.ReadFile(m.inputSpecFile)
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{
				"name": name,
				"file": m.inputSpecFile,
			}).Error("file option: reading file")
		return err, nil, nil
	}

	err = json.Unmarshal(specByteStream, &m.inputSpec.specSet)
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{
				"name": name,
				"file": m.inputSpecFile,
			}).Error("file option: parsing JSON metric spec")
		return err, nil, nil
	}

	m.output, err = nc.config.GetString(name, "output")
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{
				"name":    name,
				"options": metricsTypeMap}).Error(
			"output option: required")
		return err, nil, nil
	}

	outputType, ok := metricsTypeMap[m.output]
	if !ok {
		err = fmt.Errorf(
			"invalid 'output' [%s], must be one of [%v]",
			m.output, metricsTypeMap)
		logger.WithError(err).WithFields(
			log.Fields{
				"name":    name,
				"options": metricsTypeMap}).Error(
			"output option: unsupported")
		return err, nil, nil
	}

	//
	// Let output handler set itself up.
	switch outputType {
	case metricsTypePrometheus:
		m.outputHandler, err = metricsPrometheusNew(name, nc)
		if err != nil {
			//
			// Errors logged in called function
			return err, nil, nil
		}
	case metricsTypeInflux:
		m.outputHandler, err = metricsInfluxNew(name, nc)
		if err != nil {
			//
			// Errors logged in called function
			return err, nil, nil
		}
	case metricsTypeTest:
		m.outputHandler = testOutputHandler
		if m.outputHandler == nil {
			return fmt.Errorf("test metric extraction unset"), nil, nil
		}

	default:
		logger.WithFields(
			log.Fields{
				"name":   name,
				"output": m.output,
			}).Error("output option: failed to setup")
		return err, nil, nil
	}

	err = m.buildSpecDB()
	if err != nil {
		//
		// Logging in called function.
		return err, nil, nil
	}

	//
	// Check if we are tracking statistics on collections
	// up.
	m.inputSpec.stats = &stat
	stat.statSensorCount, _ = nc.config.GetInt(name, "statsensorcount")
	if stat.statSensorCount != 0 {
		stat.statRecords = make(map[string]*statRecord, stat.statSensorCount)
		//
		// We want to clean stale statRecord entries to make room for
		// new ones.
		stat.name = name
		stat.evict = make(chan bool, 1)
		go stat.scheduleEviction()
	}

	m.dataChannelDepth, err = nc.config.GetInt(name, "datachanneldepth")
	if err != nil {
		m.dataChannelDepth = DATACHANNELDEPTH
	}

	logger.WithFields(
		log.Fields{
			"name":       name,
			"output":     m.output,
			"file":       m.inputSpecFile,
			"metricSpec": m.inputSpec,
		}).Debug("metrics export configured")

	//
	// Setup control and data channels
	m.ctrlChan = make(chan *ctrlMsg)
	m.dataChan = make(chan dataMsg, m.dataChannelDepth)
	m.shutdownChan = make(chan struct{})

	go m.controlLoop()

	return nil, m.dataChan, m.ctrlChan
}

func metricsSetupDumpfile(filename string, logctx *log.Entry) (
	*os.File, *bufio.Writer) {

	dumpfile, err := os.OpenFile(
		filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		logctx.WithError(err).WithFields(log.Fields{
			"metricsDumpFilename": filename,
		}).Error("Setting up metrics debug dump file")
		return nil, nil
	}

	dumpwriter := bufio.NewWriter(dumpfile)

	return dumpfile, dumpwriter
}

type metricsStatMonitorType struct {
	deltaTS              *prometheus.SummaryVec
	outOfSeq             *prometheus.CounterVec
	statCacheFull        *prometheus.CounterVec
	basePathMetricsError *prometheus.CounterVec
}

var metricsStatMonitor metricsStatMonitorType

func init() {

	metricsStatMonitor = metricsStatMonitorType{
		deltaTS: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: "timestamp_delta",
				Help: "Summary for timestamp delta between updates",
			},
			[]string{"key"}),
		outOfSeq: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "outofseq",
				Help: "Out Of Sequence Timestamps",
			},
			[]string{"key"}),
		statCacheFull: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "statCacheFull",
				Help: "Exhausted stats record cache",
			},
			[]string{"key"}),
		basePathMetricsError: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "metrics_base_path_error",
				Help: "Counter tracking metrics errors per-base_path",
			},
			[]string{"section", "source", "base_path", "errortype"}),
	}

	prometheus.MustRegister(metricsStatMonitor.deltaTS)
	prometheus.MustRegister(metricsStatMonitor.outOfSeq)
	prometheus.MustRegister(metricsStatMonitor.statCacheFull)
	prometheus.MustRegister(metricsStatMonitor.basePathMetricsError)
}
