//
// June 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//

package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	telem "github.com/cisco/bigmuddy-network-telemetry-proto/proto_go"
	"github.com/dlintw/goconf"
	"testing"
	"time"
)

func TestDataMsgRouter(t *testing.T) {

	var nc nodeConfig
	var err error
	var next, last_attempts, blocked int
	var testSrc dataMsgRouterTestSRC

	nc.config, err = goconf.ReadConfigFile("pipeline_test.conf")
	if err != nil {
		t.Fatalf("Failed to read config [%v]", err)
	}

	logctx := logger.WithFields(log.Fields{
		"name": "TEST_MESSAGE_ROUTER",
	})

	workers := 3
	shutChan := make(chan struct{})
	outChans := make([]chan dataMsg, workers)
	for i := 0; i < workers; i++ {
		outChans[i] = make(chan dataMsg, 1)
	}
	inChan := make(chan dataMsg, 1000)

	next = 0
	last_attempts = 0

	msg_router := &dataMsgRouter{
		dataChanIn:   inChan,
		shutdownChan: shutChan,
		dataChansOut: outChans,
		logctx:       logctx,
		route: func(msg dataMsg, attempts int) int {
			last_attempts = attempts
			// fmt.Println("\t\tRouting (next/attempts): ", next, attempts)
			return next
		},
		handleCongested: func(msg dataMsg, attempts int, worker int) dataMsgRouterCongestionAction {
			// Reroute to another worker.
			if attempts < workers {
				next = next + 1
				if next == workers {
					next = 0
				}
				// fmt.Println("\t\tRerouting: ", next)
				return DATAMSG_ROUTER_REROUTE
			}

			// fmt.Println("Block (attempts/workers): ", attempts)
			blocked++
			return DATAMSG_ROUTER_SEND_AND_BLOCK
		},
		// We do not really use the timeout. Behaviour is currently to
		// hunt for worker whcih can take message or drop.
		timeout: time.Duration(1) * time.Second,
	}

	logctx = logctx.WithFields(log.Fields{"workers": workers})

	go msg_router.run()

	msg := &dataMsgGPB{
		source:       &testSrc,
		cachedDecode: &telem.Telemetry{EncodingPath: "a.b.c"},
	}
	//
	// Produce to the router
	inChan <- msg
	time.Sleep(1 * time.Second)
	rx := <-outChans[0]
	if rx != msg {
		t.Fatalf("Failed to receive expected message from first worker")
	}

	if last_attempts != 0 {
		t.Fatalf("Last attempts, expected 0 retries, got %d", last_attempts)
	}

	//
	// Produce twice causing congestion, and rerouting
	inChan <- msg
	inChan <- msg

	//
	// Wait, before we start draining, let's make sure the first
	// catches up with the second - we are testing hunting.
	time.Sleep(1 * time.Second)

	rx = <-outChans[0]
	if rx != msg {
		t.Fatalf("Failed to receive expected message from first worker")
	}

	rx = <-outChans[1]
	if rx != msg {
		t.Fatalf("Failed to receive expected message from second worker")
	}

	if next != 1 {
		t.Fatalf("Should have moved to next worker, expected 1, got %d", next)
	}

	if last_attempts != 1 {
		t.Fatalf("Last attempts, expected 1st retry, got %d", last_attempts)
	}

	count := 10 * workers
	received := 0
	ready := make(chan struct{})

	go func() {
		// Give us time to flood and block
		time.Sleep(3 * time.Second)

		w_handled := make([]int, workers)
		w := 0
		for {
			_, ok := <-outChans[w]
			if ok {
				received++
				w_handled[w]++
				if received == count {
					break
				}
			}
			w++
			if w == workers {
				w = 0
			}
		}
		for w = 0; w < workers; w++ {
			fmt.Printf("worker %d handled %d\n", w, w_handled[w])
		}
		close(ready)
	}()

	for k := 0; k < count; k++ {
		inChan <- msg
	}

	time.Sleep(1 * time.Second)
	close(shutChan)
	<-ready
	fmt.Println("Blocked: ", blocked)
	if count != received {
		t.Fatalf("Expected %d, got %d when testing with send and block",
			count, received)
	}
}

type dataMsgRouterTestSRC struct {
}

func (m *dataMsgRouterTestSRC) String() string {
	return "msg router test src"
}
