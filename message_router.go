//
// June 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//

package main

import (
	log "github.com/sirupsen/logrus"
	"time"
)

//
// dataMsgRouter is a router of dataMsgs (collects from one in
// channel, and routes to one of a number of output channels).
// The routing decision algorithm is parameterised and dictated by the
// owner. Behaviour on congestion is also parameterised
type dataMsgRouter struct {
	shutdownChan    chan struct{}
	dataChanIn      chan dataMsg
	dataChansOut    []chan dataMsg
	route           func(dataMsg, int) int
	handleCongested func(dataMsg, int, int) dataMsgRouterCongestionAction
	timeout         time.Duration
	logctx          *log.Entry
}

type dataMsgRouterCongestionAction int

const (
	DATAMSG_ROUTER_DROP = iota
	DATAMSG_ROUTER_REROUTE
	DATAMSG_ROUTER_SEND_AND_BLOCK
)

func (r *dataMsgRouter) handleMsg(msg dataMsg, timeout *time.Timer) {

	for i := 0; true; i++ {

		outChanIndex := r.route(msg, i)
		if len(r.dataChansOut[outChanIndex]) < cap(r.dataChansOut[outChanIndex]) {
			// Easy optimisation. No need to mess with timers. Just hand it on.
			r.dataChansOut[outChanIndex] <- msg
			return
		}

		//
		// Channel backed up and we're about to block. Check whether to block
		// or drop.
		switch r.handleCongested(msg, i, outChanIndex) {
		case DATAMSG_ROUTER_REROUTE:
			// Do be careful when rerouting to make sure that you do indeed
			// reroute.
			continue
		case DATAMSG_ROUTER_DROP:
			r.logctx.Debug("message router drop")
			return
		case DATAMSG_ROUTER_SEND_AND_BLOCK:
			//
			// We are going to send and block, or timeout.
			timeout.Reset(r.timeout)
			select {
			case r.dataChansOut[outChanIndex] <- msg:
				//
				// Message shipped. Clean out timer, and get out.
				if !timeout.Stop() {
					<-timeout.C
				}
				return
			case <-timeout.C:
				//
				// Let go round one more time.
			}
		}
	}
}

func (r *dataMsgRouter) run() {

	r.logctx.Debug("dataMsg router running")

	//
	// Setup stopped timer once.
	timeout := time.NewTimer(r.timeout)
	timeout.Stop()

	for {
		select {
		case <-r.shutdownChan:
			//
			// We're done, and queues all drained
			r.logctx.Debug("dataMsg router shutting down")
			//
			// Drain queues. We don't currently close the dataChan
			// before we send shutdown on ctrl chan, but we do
			// unhook input stages. Service as many as there are in
			// queue.
			drain := len(r.dataChanIn)
			for i := 0; i < drain; i++ {
				r.handleMsg(<-r.dataChanIn, timeout)
			}
			for _, c := range r.dataChansOut {
				//
				// conventional pattern to serialise consuming last
				// batch of messages, then shutting down.
				close(c)
			}
			r.logctx.Debug("dataMsg router shut down")
			return

		case msg := <-r.dataChanIn:
			r.handleMsg(msg, timeout)
		}
	}
}
