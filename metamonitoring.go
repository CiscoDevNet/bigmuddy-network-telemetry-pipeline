//
// May 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
package main

//
// Monitoring the pipeline. The intention is to provide services to the rest of
// pipeline bits to keep stats for the various components. We start out with
// the aim of having stats scraped by prometheus.
//
import (
	log "github.com/sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
)

var pipelineMetaMonitoringStarted bool

func metamonitoring_init(nc nodeConfig) {
	//
	// Set up server to serve metrics to prometheus
	if pipelineMetaMonitoringStarted {
		return
	}
	pipelineMetaMonitoringStarted = true

	path, err := nc.config.GetString("default",
		"metamonitoring_prometheus_resource")
	if err != nil {
		logger.Info("Metamonitoring: not enabled")
		return
	}

	server, err := nc.config.GetString("default",
		"metamonitoring_prometheus_server")
	if err != nil {
		server = ":8080"
	}

	logger.WithFields(log.Fields{
		"resource": path,
		"name":     "default",
		"server":   server}).Info(
		"Metamonitoring: serving pipeline metrics to prometheus")

	http.Handle(path, prometheus.Handler())
	err = http.ListenAndServe(server, nil)
	logger.WithError(err).Error("Metamonitoring: stop serving metrics")
}
