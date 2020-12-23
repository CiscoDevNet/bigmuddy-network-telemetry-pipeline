//
// November 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Input node used to replay streaming telemetry archives. Archives
// can be recoded using 'tap' output module with "raw = true" set.
//
// Tests for replay module are in tap_test.go
//
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"time"
)

const (
	REPLAY_DELAY_DEFAULT_USEC = 200000
)

//
// Module implementing inputNodeModule interface allowing REPLAY to read
// binary dump for replay.
type replayInputModule struct {
	name      string
	logctx    *log.Entry
	filename  string
	logData   bool
	firstN    int
	loop      bool
	delayUsec int
	done      bool
	count     int
	ctrlChan  chan *ctrlMsg
	dataChans []chan<- dataMsg
}

func replayInputModuleNew() inputNodeModule {
	return &replayInputModule{}
}

func (t *replayInputModule) String() string {
	return fmt.Sprintf("%s:%s", t.name, t.filename)
}
func (t *replayInputModule) replayInputFeederLoop() error {

	var stats msgStats
	var tickIn time.Duration
	var tick <-chan time.Time

	err, parser := getNewEncapParser(t.name, "st", t)
	if err != nil {
		t.logctx.WithError(err).Error(
			"Failed to open get parser, STOP")
		t.done = true
	}

	f, err := os.Open(t.filename)
	if err != nil {
		t.logctx.WithError(err).Error(
			"Failed to open file with binary dump of telemetry. " +
				"Dump should be produced with 'tap' output, 'raw=true', STOP")
		t.done = true
	} else {
		defer f.Close()

		if t.delayUsec != 0 {
			tickIn = time.Duration(t.delayUsec) * time.Microsecond
		} else {
			//
			// We still tick to get control channel a look in.
			tickIn = time.Nanosecond
		}
		tick = time.Tick(tickIn)
	}

	for {

		select {

		case <-tick:

			if t.done {
				// Waiting for exit, we're done here.
				continue
			}

			// iterate until a message is produced (header, payload)
			var i int
			for {
				i = i + 1
				err, buffer := parser.nextBlockBuffer()
				if err != nil {
					t.logctx.WithError(err).WithFields(
						log.Fields{
							"iteration": i,
							"nth_msg":   t.count,
						}).Error("Failed to fetch buffer, STOP")
					t.done = true
					return err
				}

				readn, err := io.ReadFull(f, *buffer)
				if err != nil {
					if err != io.EOF {
						t.logctx.WithError(err).WithFields(
							log.Fields{
								"iteration": i,
								"nth_msg":   t.count,
							}).Error("Failed to read next buffer, STOP")
						t.done = true
						return err
					}

					if !t.loop {
						//
						// We're done.
						return nil
					}

					t.logctx.Debug("restarting from start of message archive")
					_, err := f.Seek(0, 0)
					if err != nil {
						t.logctx.WithError(err).WithFields(
							log.Fields{
								"iteration": i,
								"nth_msg":   t.count,
							}).Error("Failed to go back to start, STOP")
						t.done = true
						return err
					}
					//
					// Because we're starting from scratch, and we
					// need to get a new parser to restart parser
					// state machine along with data.
					err, parser = getNewEncapParser(t.name, "st", t)
					continue
				}
				err, msgs := parser.nextBlock(*buffer, nil)
				if err != nil {
					t.logctx.WithError(err).WithFields(
						log.Fields{
							"iteration": i,
							"nth_msg":   t.count,
							"read_in":   readn,
							"len":       len(*buffer),
							"msg":       hex.Dump(*buffer),
						}).Error(
						"Failed to decode next block, STOP")
					t.done = true
					return err
				}

				if t.logData {
					t.logctx.WithFields(log.Fields{
						"iteration":    i,
						"nth_msg":      t.count,
						"dataMsgCount": len(msgs),
						"len":          len(*buffer),
						"msg":          hex.Dump(*buffer),
					}).Debug("REPLAY input logdata")
				}

				if msgs == nil {
					//
					// We probably just read a header
					continue
				}

				for _, msg := range msgs {
					for _, dataChan := range t.dataChans {
						if len(dataChan) == cap(dataChan) {
							t.logctx.Error("Input overrun (replace with counter)")
							continue
						}
						dataChan <- msg
					}
				}

				t.count = t.count + 1
				if t.count == t.firstN && t.firstN != 0 {
					t.logctx.Debug("dumped all messages expected")
					t.done = true
				}

				break
			}

		case msg := <-t.ctrlChan:
			switch msg.id {
			case REPORT:
				t.logctx.Debug("report request")
				content, _ := json.Marshal(stats)
				resp := &ctrlMsg{
					id:       ACK,
					content:  content,
					respChan: nil,
				}
				msg.respChan <- resp

			case SHUTDOWN:
				t.logctx.Info("REPLAY input loop, rxed SHUTDOWN, shutting down")

				resp := &ctrlMsg{
					id:       ACK,
					respChan: nil,
				}
				msg.respChan <- resp

				return nil

			default:
				t.logctx.Error("REPLAY input loop, unknown ctrl message")
			}
		}
	}
}

func (t *replayInputModule) replayInputFeederLoopSticky() {
	t.logctx.Debug("Starting REPLAY feeder loop")
	for {
		err := t.replayInputFeederLoop()
		if err == nil {
			t.logctx.Debug("REPLAY feeder loop done, exit")
			break
		} else {
			// retry
			time.Sleep(time.Second)
			if t.done {
				t.logctx.WithFields(log.Fields{
					"nth_msg": t.count,
				}).Debug("idle, waiting for shutdown")
			} else {
				t.logctx.Debug("Restarting REPLAY feeder loop")
			}
		}
	}
}

func (t *replayInputModule) configure(
	name string,
	nc nodeConfig,
	dataChans []chan<- dataMsg) (error, chan<- *ctrlMsg) {

	var err error

	t.filename, err = nc.config.GetString(name, "file")
	if err != nil {
		return err, nil
	}

	t.name = name
	t.logData, _ = nc.config.GetBool(name, "logdata")
	t.firstN, _ = nc.config.GetInt(name, "firstn")
	if t.firstN == 0 {
		t.loop, _ = nc.config.GetBool(name, "loop")
	}

	t.delayUsec, err = nc.config.GetInt(name, "delayusec")
	if err != nil {
		//
		// Default to a sensible-ish value for replay to
		// avoid overwhelming output stages. Note that
		// we can still overwhelm output stages if we
		// want to do so explicitly i.e. set to zero.
		t.delayUsec = REPLAY_DELAY_DEFAULT_USEC
	}

	t.ctrlChan = make(chan *ctrlMsg)
	t.dataChans = dataChans

	t.logctx = logger.WithFields(log.Fields{
		"name":      t.name,
		"file":      t.filename,
		"logdata":   t.logData,
		"firstN":    t.firstN,
		"delayUsec": t.delayUsec,
		"loop":      t.loop,
	})

	go t.replayInputFeederLoopSticky()

	return nil, t.ctrlChan
}
