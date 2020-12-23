//
// January 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	cluster "gopkg.in/bsm/sarama-cluster.v2"
	"io/ioutil"
	"regexp"
	"strings"
	"text/template"
	"time"
)

const (
	KAFKATOPICDEFAULT     = "telemetry"
	KAFKATOPICMAXLEN      = 250
	KAFKARECONNECTTIMEOUT = 1
	KAFKACHECKPOINTFLUSH  = 1
)

type kafkaKeySpec int

//
// kafkaTopicExtractor is the signature of a method used to extract a
// topic. Closure used with statically configured topics of template
// extraction.
type kafkaTopicExtractor func(*dataMsgMetaData) string

const (
	KAFKA_KEY_SPEC_NONE = iota
	//
	// This spec forces ID and PATH into key. This is here to
	// support esoteric requirement by consumer.
	KAFKA_KEY_SPEC_ID_AND_PATH
	//
	// This spec used ID as key.
	KAFKA_KEY_SPEC_ID
)

var keyExtractionMethod = map[string]kafkaKeySpec{
	"":            KAFKA_KEY_SPEC_NONE,
	"path_and_id": KAFKA_KEY_SPEC_ID_AND_PATH,
	"id":          KAFKA_KEY_SPEC_ID,
}

type kafkaProducerConfig struct {
	name         string
	fetchTopic   kafkaTopicExtractor
	brokerList   []string
	keySpec      kafkaKeySpec
	streamSpec   *dataMsgStreamSpec
	requiredAcks sarama.RequiredAcks
	logData      bool
	logCtx       *log.Entry
	stats        msgStats
}

type kafkaConsumerConfig struct {
	name          string
	topic         string
	consumerGroup string
	brokerList    []string
	keySpec       kafkaKeySpec
	msgEncoding   encoding
	logData       bool
}

//
// kafkaProducerTopicExtractor provides the closure required to
// extract the topic. This could be static, or template based
// operating on message meta data. Eventually, this may be extended to
// operate on body.
func kafkaProducerTopicExtractor(
	topic string, template *template.Template) kafkaTopicExtractor {

	if template != nil {

		validate, _ := regexp.Compile("[^a-zA-Z0-9\\._\\-]+")
		return func(m *dataMsgMetaData) string {
			if m != nil {
				var b bytes.Buffer
				err := template.Execute(&b, m)
				if err == nil {
					topic := b.String()
					//
					// Post process string for topic rules:
					// i.e. < 250 chars, and matching [a-zA-Z0-9\\._\\-]
					if len(topic) > KAFKATOPICMAXLEN {
						topic = topic[:KAFKATOPICMAXLEN]
					}
					return validate.ReplaceAllLiteralString(topic, "___")
				}
				// default to returning topic when template execution
				// fails.
				return topic
			} else {
				// Provide a description of topic when no meta data is
				// passed.
				return fmt.Sprintf("metadata template [%v]", template.Name())
			}
		}
	} else {
		return func(*dataMsgMetaData) string {
			return topic
		}
	}
}

func (cfg *kafkaProducerConfig) dataMsgToKafkaMessage(imsg dataMsg) (
	error, *sarama.ProducerMessage, *string, *string, *[]byte) {
	var key string

	topic := cfg.fetchTopic(imsg.getMetaData())
	//
	// Ask for the stream in the desired format
	err, rawstream := imsg.produceByteStream(cfg.streamSpec)
	if err != nil {
		cfg.logCtx.WithError(err).WithFields(log.Fields{
			"msg":   imsg.getDataMsgDescription(),
			"topic": topic,
		}).Error("kafka feeder loop, failed message")
		return err, nil, nil, &topic, nil
	} else if rawstream == nil {
		return nil, nil, nil, &topic, nil
	}

	switch cfg.keySpec {

	case KAFKA_KEY_SPEC_ID_AND_PATH:
		//
		// Used in customer demo and expected to be
		// deprecated.
		err, identifier := imsg.getMetaDataIdentifier()
		if err != nil {
			identifier = ""
		}

		err, path := imsg.getMetaDataPath()
		if err != nil {
			path = ""
		}

		key = path + ":" + identifier

	case KAFKA_KEY_SPEC_ID:
		err, key = imsg.getMetaDataIdentifier()
		if err != nil {
			cfg.logCtx.WithError(err).WithFields(log.Fields{
				"msg": imsg.getDataMsgDescription(),
			}).Error("kafka feeder loop, " +
				"failed to extract ID key")
		}
	case KAFKA_KEY_SPEC_NONE:
		// This is fine. We don't have to have a key.
	}

	return nil, &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(rawstream),
	}, &key, &topic, &rawstream

}

//
// Loop taking content on channel and pushing out to kafka
func (cfg *kafkaProducerConfig) kafkaFeederLoop(
	dataChan <-chan dataMsg,
	ctrlChan <-chan *ctrlMsg,
	msgproducer sarama.SyncProducer) {

	stats := &cfg.stats
	logCtx := cfg.logCtx
	logCtx.Info("kafka producer configured")

	for {
		select {
		case msg, ok := <-dataChan:

			if !ok {
				// Channel has been closed. Our demise
				// is near. SHUTDOWN is likely to be
				// received soon on control
				// channel. While the only message we
				// receive is shutdown, this could
				// have done without a control channel
				// and instead should have used range
				// over the channel.
				//
				dataChan = nil
				continue
			}

			err, kafkamsg, key, topic, rawstream :=
				cfg.dataMsgToKafkaMessage(msg)
			if err != nil {
				// event logged in called function, need to count
				stats.MsgsNOK++
				continue
			} else if kafkamsg == nil {
				// count it?
				continue
			}

			partition, offset, err := msgproducer.SendMessage(kafkamsg)

			if err != nil {
				stats.MsgsNOK++
				kafkaMetaMonitor.CountersErrors.WithLabelValues(
					cfg.name, *topic, *key, "out").Inc()
				logMsgCtx := logCtx.WithError(err).WithFields(log.Fields{
					"msg":     msg.getDataMsgDescription(),
					"topic":   *topic,
					"key":     *key,
					"content": hex.Dump(*rawstream),
				})
				if cfg.logData {
					logMsgCtx = logMsgCtx.WithFields(log.Fields{
						"content": hex.Dump(*rawstream),
					})
				}
				logMsgCtx.WithError(err).Error(
					"kafka feeder loop, failed message")

			} else {
				stats.MsgsOK++
				kafkaMetaMonitor.CountersMsgs.WithLabelValues(
					cfg.name, *topic, *key, "out").Inc()
				kafkaMetaMonitor.CountersBytes.WithLabelValues(
					cfg.name, *topic, *key, "out").Add(
					float64(len(*rawstream)))
				if cfg.logData {
					logCtx.WithFields(
						log.Fields{
							"msg":       msg.getDataMsgDescription(),
							"key":       *key,
							"partition": partition,
							"offset":    offset,
							"topic":     *topic,
							"content":   hex.Dump(*rawstream)}).Debug(
						"kafka feeder loop, posted message")
				}
			}

		case msg := <-ctrlChan:
			if cfg.kafkaHandleCtrlMsg(msg) {
				//
				// Close connection to kafka. Should we drain the
				// dataChan first?  Probably.
				if err := msgproducer.Close(); err !=

					nil {
					logCtx.WithError(err).Error(
						"Failed to shut down producer cleanly")
				}
				return
			}
		}
	}
}

func (cfg *kafkaProducerConfig) kafkaHandleCtrlMsg(msg *ctrlMsg) bool {

	switch msg.id {

	case REPORT:
		content, _ := json.Marshal(cfg.stats)
		resp := &ctrlMsg{
			id:       ACK,
			content:  content,
			respChan: nil,
		}
		msg.respChan <- resp

	case SHUTDOWN:
		cfg.logCtx.Info("kafka producer rxed SHUTDOWN")

		resp := &ctrlMsg{
			id:       ACK,
			respChan: nil,
		}
		msg.respChan <- resp
		return true

	default:
		cfg.logCtx.Error("kafka producer, unknown ctrl message")
	}

	return false
}

//
// kafkaFeederLoopSticky handles setting up the kafka sync produces as
// often as possible.
func (cfg *kafkaProducerConfig) kafkaFeederLoopSticky(
	dataChan <-chan dataMsg,
	ctrlChan <-chan *ctrlMsg) {

	var msgproducer sarama.SyncProducer
	var err error

	for {
		config := sarama.NewConfig()
		config.Producer.RequiredAcks = cfg.requiredAcks
		config.Producer.Return.Successes = true
		msgproducer, err = sarama.NewSyncProducer(cfg.brokerList, config)
		if err != nil {
			cfg.logCtx.WithError(err).Error(
				"kafka producer setup (will retry)")
			select {
			case <-time.After(KAFKARECONNECTTIMEOUT * time.Second):
				// Time to try again
				break
			case msg := <-ctrlChan:
				if cfg.kafkaHandleCtrlMsg(msg) {
					//
					// We're going down. Given up and leave.
					return
				}
			}
			continue
		} else {
			break
		}
	}
	//
	// We're all setup and ready to go... never back until we're
	// done.
	cfg.kafkaFeederLoop(dataChan, ctrlChan, msgproducer)
}

//
// Module implementing outputNodeModule interface.
type kafkaOutputModule struct{}

func kafkaOutputModuleNew() outputNodeModule {
	return &kafkaOutputModule{}
}

//
// Setup a kafka producer and get back a data and control channel
func (k *kafkaOutputModule) configure(name string, nc nodeConfig) (
	error, chan<- dataMsg, chan<- *ctrlMsg) {

	var topicTemplate *template.Template
	var requiredAcks sarama.RequiredAcks

	logctx := logger.WithFields(log.Fields{"name": name})

	brokers, err := nc.config.GetString(name, "brokers")
	if err != nil {
		logctx.WithError(err).Error(
			"kafka producer setup, broker config required")
		return err, nil, nil
	}
	brokerList := strings.Split(brokers, ",")

	dataChannelDepth, err := nc.config.GetInt(name, "datachanneldepth")
	if err != nil {
		dataChannelDepth = DATACHANNELDEPTH
	}

	//
	// Setup topic handling.
	topicTemplateFileName, err := nc.config.GetString(
		name, "topic_metadata_template")
	if err == nil {
		//
		// A template has been provided. Lets make sure we can load
		// and parse it.
		templateSpec, err := ioutil.ReadFile(topicTemplateFileName)
		if err != nil {
			logctx.WithError(err).Error(
				"kafka producer setup, topic template load fail")
			return err, nil, nil
		}
		topicTemplate, err = template.New(name).Parse(string(templateSpec))
		if err != nil {
			logctx.WithError(err).Error(
				"kafka producer setup, topic template parse fail")
			return err, nil, nil
		}
	}

	topic, err := nc.config.GetString(name, "topic")
	if err != nil {
		topic = KAFKATOPICDEFAULT
	}
	topicExtractor := kafkaProducerTopicExtractor(topic, topicTemplate)

	keyopt, err := nc.config.GetString(name, "key")
	if err != nil {
		keyopt = ""
	}

	// Pick default output stream
	err, streamSpec := dataMsgStreamSpecFromConfig(nc, name)
	if err != nil {
		logctx.WithError(err).Error(
			"'encoding' option for kafka output")
		return err, nil, nil
	}

	//
	// If not set, will default to false, but let's be clear.
	logData, _ := nc.config.GetBool(name, "logdata")
	if err != nil {
		logData = false
	}

	keySpec, ok := keyExtractionMethod[keyopt]
	if !ok {
		err = fmt.Errorf("kafka: key not a valid value")
		logctx.WithError(err).WithFields(
			log.Fields{
				"brokers":         brokerList,
				"key":             keyopt,
				"expected one of": keyExtractionMethod}).Error(
			"kafka producer setup")
		return err, nil, nil
	}

	//
	// Reliability setup
	requiredAcksString, err := nc.config.GetString(name, "required_acks")
	if err != nil {
		// Explicit default
		requiredAcks = sarama.NoResponse
	} else {
		switch {
		case requiredAcksString == "none":
			requiredAcks = sarama.NoResponse
		case requiredAcksString == "local":
			requiredAcks = sarama.WaitForLocal
		case requiredAcksString == "commit":
			requiredAcks = sarama.WaitForAll
		default:
			logctx.WithError(err).Error(
				"'required_acks' option expects 'local' or 'commit'")
		}
	}

	logctx = logctx.WithFields(
		log.Fields{
			"name":         name,
			"topic":        topicExtractor(nil),
			"brokers":      brokerList,
			"streamSpec":   streamSpec,
			"requiredAcks": requiredAcks,
		})
	//
	// track config in struct
	cfg := &kafkaProducerConfig{
		name:         name,
		brokerList:   brokerList,
		fetchTopic:   topicExtractor,
		keySpec:      keySpec,
		streamSpec:   streamSpec,
		requiredAcks: requiredAcks,
		logData:      logData,
		logCtx:       logctx,
	}

	// Create the required channels; a sync ctrl channel and a data channel.
	ctrlChan := make(chan *ctrlMsg)
	dataChan := make(chan dataMsg, dataChannelDepth)

	go cfg.kafkaFeederLoopSticky(dataChan, ctrlChan)

	return nil, dataChan, ctrlChan
}

//
// Module implementing inputNodeModule interface.
type kafkaInputModule struct {
	name string
}

func kafkaInputModuleNew() inputNodeModule {
	return &kafkaInputModule{}
}

func (k *kafkaInputModule) handleCtrlMsg(
	msg *ctrlMsg,
	consumer *cluster.Consumer,
	logCtx *log.Entry,
	stats msgStats) bool {

	done := false

	resp := &ctrlMsg{
		id:       ACK,
		respChan: nil,
	}

	switch msg.id {
	case REPORT:
		resp.content, _ = json.Marshal(stats)

	case SHUTDOWN:
		logCtx.Info("kafka consumer rxed SHUTDOWN")

		if consumer != nil {
			if err := consumer.Close(); err != nil {
				logCtx.WithError(err).Error(
					"kafka consumer shutdown")
			}
		}

		done = true

	default:
		logCtx.Error("kafka consumer, unknown ctrl message")
	}

	msg.respChan <- resp

	return done
}

type kafkaRemoteProducer struct {
	remoteProducer string
}

func (s *kafkaRemoteProducer) String() string {
	return s.remoteProducer
}

func (k *kafkaInputModule) extractKafkaProducer(
	cfg *kafkaConsumerConfig,
	msg *sarama.ConsumerMessage,
) msgproducer {

	if msg.Key != nil && len(msg.Key) != 0 {

		switch cfg.keySpec {

		case KAFKA_KEY_SPEC_ID:
			return &kafkaRemoteProducer{remoteProducer: string(msg.Key)}

		case KAFKA_KEY_SPEC_ID_AND_PATH:
			//
			// key is in form path:id
			keypart := strings.Split(string(msg.Key), ":")
			if len(keypart) == 2 {
				return &kafkaRemoteProducer{
					remoteProducer: keypart[1],
				}
			}
		}
	}

	//
	// Last resort producer
	return &kafkaRemoteProducer{remoteProducer: k.name}
}

func (k *kafkaInputModule) maintainKafkaConsumerConnection(
	cfg *kafkaConsumerConfig,
	ctrlChan <-chan *ctrlMsg,
	dataChans []chan<- dataMsg,
) {
	var consumer *cluster.Consumer
	var client *cluster.Client
	var notifications <-chan *cluster.Notification

	var errorChan <-chan error

	var msgChan <-chan *sarama.ConsumerMessage
	var checkpointMsg *sarama.ConsumerMessage
	var stats msgStats

	//
	// Timeout channel
	timeout := make(chan bool, 1)

	//
	// Checkpoint offset timer
	checkpoint_flush := make(chan bool, 1)
	checkpoint_flush_scheduled := false

	//
	// Setup logging context once
	logCtx := logger.WithFields(
		log.Fields{
			"name":     k.name,
			"topic":    cfg.topic,
			"group":    cfg.consumerGroup,
			"brokers":  cfg.brokerList,
			"encoding": encodingToName(cfg.msgEncoding),
		})

	//
	// Setup codec according to configuration
	err, codec := getCodec(k.name, cfg.msgEncoding)
	if err != nil {
		logCtx.WithError(err).Error(
			"kafka consumer unsupported encoding (fatal)")
		return
	}

	logCtx.Info("kafka consumer configured")
	for {

		//
		// Get a default configuration
		clusterConfig := cluster.NewConfig()

		//
		// Register to receive notifications on rebalancing of
		// partition to consumer mapping. This allows us to
		// have multiple consumers in consumer group, and
		// allows us to adapt as we add and remove such
		// consumers.
		clusterConfig.Group.Return.Notifications = true
		clusterConfig.Consumer.Return.Errors = true

		client, err = cluster.NewClient(cfg.brokerList, clusterConfig)
		if err != nil {
			logCtx.WithError(err).Error(
				"kafka consumer client connect (will retry)")
			//
			// We will setup restart further down if this fails.
		}

		if client != nil {
			consumer, err = cluster.NewConsumerFromClient(
				client, cfg.consumerGroup, []string{cfg.topic})

			if err != nil {
				logCtx.WithError(err).Error(
					"kafka consumer create (will retry)")
			}
		}

		if consumer != nil {
			//
			// Listen to notifications, for visibility only
			notifications = consumer.Notifications()

			//
			// Listen to and log errors (eventually we may handle
			// differently)
			errorChan = consumer.Errors()
			//
			// Listen for messages... this is where we
			// will do the heavy lifting bit.
			msgChan = consumer.Messages()

			logCtx.WithFields(log.Fields{
				"subscriptions": consumer.Subscriptions(),
			}).Info("kafka consumer connected")
		} else {
			//
			// Go into select loop, with scheduled retry
			// in a second. Note that this will make sure we
			// remain responsive and handle cleanup signal
			// correctly if necessary.
			go func() {
				time.Sleep(
					KAFKARECONNECTTIMEOUT * time.Second)
				timeout <- true
			}()
		}

		restart := false
		for {
			select {
			//
			// Handle rebalancing, shutdown, and retry in
			// this select clause
			case remoteErr := <-errorChan:
				logCtx.WithError(remoteErr).Error("kafka consumer rxed Error")

			case restart_request := <-timeout:
				// Handle timeout, simply restart the whole
				// sequence trying to connect to kafka as
				// consumer.
				if restart_request {
					logCtx.Debug("kafka consumer, attempt reconnect")
					restart = true
				}

			case <-checkpoint_flush:
				checkpoint_flush_scheduled = false
				if checkpointMsg != nil {
					consumer.MarkOffset(checkpointMsg, "ok")
				}
				checkpointMsg = nil

			case data := <-msgChan:

				checkpointMsg = data
				if !checkpoint_flush_scheduled {
					checkpoint_flush_scheduled = true
					go func() {
						time.Sleep(KAFKACHECKPOINTFLUSH * time.Second)
						checkpoint_flush <- true
					}()
				}

				err, dMs := codec.blockToDataMsgs(
					k.extractKafkaProducer(cfg, data),
					data.Value,
				)

				kafkaMetaMonitor.CountersMsgs.WithLabelValues(
					cfg.name, cfg.topic, string(data.Key), "in").Inc()
				kafkaMetaMonitor.CountersBytes.WithLabelValues(
					cfg.name, cfg.topic, string(data.Key), "in").Add(
					float64(len(data.Value)))
				if err != nil || cfg.logData {
					logMsgCtx := logCtx.WithFields(log.Fields{
						"partition": data.Partition,
						"offset":    data.Offset,
						"key":       string(data.Key),
						"len":       len(data.Value),
					})
					if cfg.logData {
						//
						// Only add hex dump if logging data.
						logMsgCtx = logMsgCtx.WithFields(log.Fields{
							"content": hex.Dump(data.Value),
						})
					}
					if err != nil {
						stats.MsgsNOK++
						logMsgCtx.WithError(err).Error(
							"kafka consumer msg")
						kafkaMetaMonitor.CountersErrors.WithLabelValues(
							cfg.name, cfg.topic, string(data.Key), "in").Inc()
					} else if cfg.logData {
						stats.MsgsOK++
						logMsgCtx.Debug("kafka consumer msg")
					}
				}

				for _, dM := range dMs {
					for i, dataChan := range dataChans {
						if cap(dataChan) == len(dataChan) {
							//
							// Data channel full; blocking.
							// If we choose to add tail drop option to avoid
							// head-of-line blocking, this is where we would
							// drop.
							logCtx.WithFields(
								log.Fields{"channel": i}).Debug(
								"kafka consumer overrun (increase 'datachanneldepth'?)")
						}
						//
						// Pass it on. Make sure we handle shutdown
						// gracefully too.
						select {
						case dataChan <- dM:
							continue
						case msg := <-ctrlChan:
							if k.handleCtrlMsg(msg, consumer, logCtx, stats) {
								return
							}
						}
					}
				}

			case notification := <-notifications:
				//
				// Rebalancing activity. Setup and
				// teardown partition readers
				// according to rebalance.
				logCtx.WithFields(log.Fields{
					"claimed":  notification.Claimed,
					"released": notification.Released,
					"current":  notification.Current,
				}).Debug("kafka consumer notification")

			case msg := <-ctrlChan:
				if k.handleCtrlMsg(msg, consumer, logCtx, stats) {
					return
				}
			}

			if restart {
				break
			}
		}

		if consumer != nil {
			if err := consumer.Close(); err != nil {
				logCtx.WithError(err).Error(
					"kafka consumer cleanup before retry")
			}
		}
		//
		// Ensure a flush does not wrap around consumer instances.
		checkpointMsg = nil
		consumer = nil
	}
}

//
// Setup a kafka consumer. A fair bit of this setup is common between
// consumer and producer and should be shared.
func (k *kafkaInputModule) configure(
	name string,
	nc nodeConfig,
	dataChans []chan<- dataMsg) (error, chan<- *ctrlMsg) {

	k.name = name

	brokers, err := nc.config.GetString(name, "brokers")
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{"name": name}).Error(
			"kafka consumer requires 'brokers'")
		return err, nil
	}
	brokerList := strings.Split(brokers, ",")

	consumerGroup, err := nc.config.GetString(name, "consumergroup")
	if err != nil {
		logger.WithError(err).WithFields(
			log.Fields{"name": name}).Error(
			"kafka consumer requires 'consumergroup' identifying consumer")
		return err, nil
	}

	topic, err := nc.config.GetString(name, "topic")
	if err != nil {
		topic = KAFKATOPICDEFAULT
	}

	// Pick default input stream type
	enc := ENCODING_JSON
	// Check for configuration override
	encodingopt, err := nc.config.GetString(name, "encoding")
	if err == nil {
		err, enc = nameToEncoding(encodingopt)
		if err != nil {
			logger.WithError(err).WithFields(
				log.Fields{"name": name}).Error(
				"kafka consumer encoding option")
			return err, nil
		}
	}

	//
	// If not set, will default to false, but let's be clear.
	logData, err := nc.config.GetBool(name, "logdata")
	if err != nil {
		logData = false
	}

	keyopt, err := nc.config.GetString(name, "key")
	if err != nil {
		keyopt = ""
	}
	keySpec, ok := keyExtractionMethod[keyopt]
	if !ok {
		err = fmt.Errorf("kafka: key not a valid value")
		logger.WithError(err).WithFields(
			log.Fields{"name": name,
				"brokers":         brokerList,
				"key":             keyopt,
				"expected one of": keyExtractionMethod}).Error(
			"kafka consumer setup, unknown message key type")
		return err, nil
	}

	//
	// track config in struct
	cfg := &kafkaConsumerConfig{
		name:          name,
		topic:         topic,
		consumerGroup: consumerGroup,
		brokerList:    brokerList,
		keySpec:       keySpec,
		msgEncoding:   enc,
		logData:       logData,
	}

	//
	// Setup logger in kafka library to feed ours for detailed
	// logging. Would need to figure out how to do this for output
	// module too. We handle sarama error messages already over error
	// channel.
	//
	// sarama.Logger = logger

	//
	// Set up control channel and kick off sticky consumer
	// connection
	ctrlChan := make(chan *ctrlMsg)
	go k.maintainKafkaConsumerConnection(cfg, ctrlChan, dataChans)

	return nil, ctrlChan
}

type kafkaMetaMonitorType struct {
	CountersMsgs   *prometheus.CounterVec
	CountersBytes  *prometheus.CounterVec
	CountersErrors *prometheus.CounterVec
}

var kafkaMetaMonitor *kafkaMetaMonitorType

func init() {

	kafkaMetaMonitor = &kafkaMetaMonitorType{
		CountersMsgs: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_messages",
				Help: "Messages",
			},
			[]string{"section", "topic", "key", "dir"}),
		CountersBytes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_bytes",
				Help: "Bytes",
			},
			[]string{"section", "topic", "key", "dir"}),
		CountersErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_errors",
				Help: "Errors",
			},
			[]string{"section", "topic", "key", "dir"}),
	}

	// Dump content
	prometheus.MustRegister(kafkaMetaMonitor.CountersMsgs)
	prometheus.MustRegister(kafkaMetaMonitor.CountersBytes)
	prometheus.MustRegister(kafkaMetaMonitor.CountersErrors)
}
