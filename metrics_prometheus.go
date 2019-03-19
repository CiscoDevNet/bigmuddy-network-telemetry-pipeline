//
// June 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Feed metrics to prometheus
//
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	log "github.com/Sirupsen/logrus"
)

//
// Specify defaults in case they are not configured.
const (
	PROMETHEUS_JOBNAME = "telemetry"
)

type metricsPrometheusOutputHandler struct {
	pushGWAddress string
	jobName       string
	instance      string
	pushURL       string
	instanceBuf   *bytes.Buffer
	//
	// metricsfilename allow for diagnostic dump of metrics as
	// exported.
	metricsfilename string
	dump            *bufio.Writer
}

//
// Prometheus constrains symbols in sensor name
func (p *metricsPrometheusOutputHandler) adaptSensorName(name string) string {

	re := regexp.MustCompile("[^a-zA-Z0-9_:]+")

	return re.ReplaceAllString(name, "_")
}

//
// Prometheus constrains symbols in tag names
func (p *metricsPrometheusOutputHandler) adaptTagName(name string) string {
	re := regexp.MustCompile("[^a-zA-Z0-9_]+")

	return re.ReplaceAllString(name, "_")
}

func (p *metricsPrometheusOutputHandler) flushMetric(
	tags []metricsAtom,
	ts uint64,
	context metricsOutputContext) {

	var pushURL string

	buf := context.(*bytes.Buffer)

	logCtx := logger.WithFields(log.Fields{
		"pushGWAddress": p.pushGWAddress,
		"jobName":       p.jobName,
		"pushURL":       p.pushURL,
		"instance":      p.instance,
	})

	if buf.Len() == 0 {
		logCtx.Debug("metrics export (no metrics in msg): keys [%v]", tags)
		return
	}

	//
	// Add unique instance to disambiguate time series for grouped
	// metrics.
	//
	// As multiple metrics make it into the push gateway they are
	// grouped by (job, instance or URL labels). Only one update per
	// group of metrics is tracked at any one time - any subsequent
	// updates to the same group prior to scraping replaces any
	// previous updates.
	//
	// In order to avoid losing updates, we make sure that every
	// unique timeseries as identified by the tags, results in a
	// distinct sequence (module hash collisions).
	p.instanceBuf.Reset()
	for i := 0; i < len(tags); i++ {
		p.instanceBuf.WriteString(
			fmt.Sprintf("%s=\"%v\" ", tags[i].key, tags[i].val))
	}
	if p.instanceBuf.Len() != 0 {
		h := fnv.New64a()
		h.Write(p.instanceBuf.Bytes())
		pushURL = fmt.Sprintf("%s_%v", p.pushURL, h.Sum64())
	} else {
		pushURL = p.pushURL
	}

	//
	// Dump a copy as a string if necessary, showing URL too.
	if p.dump != nil {
		p.dump.WriteString("POST " + pushURL + "\n")
		_, err := p.dump.WriteString(buf.String())
		if err != nil {
			logCtx.WithError(err).Error("failed dump metric to file")
		}
	}

	// POST, (not PUT), to make sure that only metrics in same group
	// are replaced.
	req, err := http.NewRequest("POST", pushURL, buf)
	if err != nil {
		logCtx.WithError(err).Error("http new request")
		return
	}

	req.Header.Set("Content-Type", `text/plain; version=0.0.4`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logCtx.WithError(err).Error("http post")
		return
	}

	if resp.StatusCode != 202 {
		err = fmt.Errorf(
			"unexpected status code %d while pushing to %s",
			resp.StatusCode, pushURL)
		logCtx.WithError(err).Error("http reply")
	}

	resp.Body.Close()

	buf.Reset()
}

func (p *metricsPrometheusOutputHandler) buildMetric(
	tags []metricsAtom,
	sensor metricsAtom,
	ts uint64,
	context metricsOutputContext) {
	var delim string

	buf := context.(*bytes.Buffer)

	buf.WriteString(sensor.key)

	if len(tags) > 0 {
		delim = "{"
		for i := 0; i < len(tags); i++ {
			buf.WriteString(
				fmt.Sprintf(
					"%s%s=\"%v\"",
					delim,
					tags[i].key,
					tags[i].val))
			if i == 0 {
				// change delim
				delim = ","
			}
		}
		delim = "} "
	} else {
		delim = " "
	}
	buf.WriteString(fmt.Sprintf("%s%v\n", delim, sensor.val))
}

func (p *metricsPrometheusOutputHandler) worker(m *metricsOutputModule) {

	var metricsfile *os.File

	//
	// We don't worry about sizing of buf for this worker. This same
	// buf will be used throughout the life of the worker. Underlying
	// storage will grow with first few message to accomodate largest
	// message built automatically. This knowledge is preserved across
	// message since we only call reset between one message and
	// another. Put another way, buf storage grows monotonically over
	// time.
	buf := new(bytes.Buffer)
	p.instanceBuf = new(bytes.Buffer)

	defer m.shutdownSyncPoint.Done()
	//
	// Start by computing the push URL to use. Using the same approach as
	// push prometheus client.
	if !strings.Contains(p.pushGWAddress, "://") {
		p.pushGWAddress = "http://" + p.pushGWAddress
	}
	if strings.HasSuffix(p.pushGWAddress, "/") {
		p.pushGWAddress = p.pushGWAddress[:len(p.pushGWAddress)-1]
	}
	p.pushURL = fmt.Sprintf(
		"%s/metrics/job/%s/instance/%s",
		p.pushGWAddress,
		url.QueryEscape(p.jobName),
		url.QueryEscape(p.instance))

	logCtx := logger.WithFields(log.Fields{
		"name":    m.name,
		"output":  m.output,
		"file":    m.inputSpecFile,
		"pushURL": p.pushURL,
	})

	if p.metricsfilename != "" {
		metricsfile, p.dump = metricsSetupDumpfile(
			p.metricsfilename, logCtx)
		if metricsfile != nil {
			defer metricsfile.Close()
		}
	}

	for {

		select {
		//
		// Look for shutdown
		case <-m.shutdownChan:
			//
			// We're being signalled to leave.
			logCtx.Info("metrics prometheus worker exiting")
			return

		//
		// Receive message
		case msg, ok := <-m.dataChan:

			if !ok {
				// Channel has been closed. Our demise is
				// near. SHUTDOWN is likely to be received soon on
				// control channel.
				//
				m.dataChan = nil
				continue
			}

			//
			// Make sure we clear any lefto over message (only on
			// error)
			buf.Reset()
			err := msg.produceMetrics(&m.inputSpec, m.outputHandler, buf)
			if err != nil {
				//
				// We should count these and export them from meta
				// monitoring
				logCtx.WithError(err).Error("message producing metrics")
				continue
			}
		}
	}
}

func (p *metricsPrometheusOutputHandler) setupWorkers(m *metricsOutputModule) {
	m.shutdownSyncPoint.Add(1)
	go p.worker(m)
}

func metricsPrometheusNew(name string, nc nodeConfig) (metricsOutputHandler, error) {

	var p metricsPrometheusOutputHandler
	var err error

	p.pushGWAddress, err = nc.config.GetString(name, "pushgw")
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{"name": name}).Error(
			"attribute 'pushgw' required for prometheus metric export")
		return nil, err
	}

	p.jobName, err = nc.config.GetString(name, "jobname")
	if err != nil {
		p.jobName = PROMETHEUS_JOBNAME
	}

	p.instance, err = nc.config.GetString(name, "instance")
	if err != nil {
		p.instance = conductor.ID
	}

	// If not set, will default to false
	p.metricsfilename, _ = nc.config.GetString(name, "dump")

	return &p, nil
}
