//
// June 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//

package main

import (
	"github.com/dlintw/goconf"
	"testing"
)

func TestMetricsInfluxConfigure(t *testing.T) {

	var nc nodeConfig
	var err error

	nc.config, err = goconf.ReadConfigFile("pipeline_test.conf")
	if err != nil {
		t.Fatalf("Failed to read config [%v]", err)
	}

	//
	// Now that a password is set, let's test the non-interactive
	// negative and positive path.
	name := "influxtest"
	nc.config.AddOption(name, "influx", "")
	_, err = metricsInfluxNew(name, nc)
	if err == nil {
		t.Fatalf("Test passed but should fail, empty influx")
	}

	nc.config.AddOption(name, "influx", "http://localhost:8086")
	_, err = metricsInfluxNew(name, nc)
	if err == nil {
		t.Fatalf("Test passed but should fail, missing database")
	}

	nc.config.AddOption(name, "database", "mydatabase")
	*pemFileName = "id_rsa_FOR_TEST_ONLY"
	pw := "mysillysillysillypassword"
	err, epw := encrypt_password(*pemFileName, []byte(pw))
	nc.config.AddOption(name, "password", epw)
	nc.config.AddOption(name, "username", "user")
	_, err = metricsInfluxNew(name, nc)
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
}
