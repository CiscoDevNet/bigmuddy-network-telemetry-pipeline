package main

//
// Notes:
//
// The life of the router is to pull its dataMsg channel, and feed it it
// to one of a number of worker queues.
//
// The life of a worker is to pull messages from it's queue, and for each,
// build metrics.
//
// A worker handles produceMetrics callbacks as follows:
// On build metric;
//  we simply accumulate metricsAtoms
// On flushMetric, we add take the tags, and the accumulated metrics, and
// build a point.
// On return from produceMetrics we write the batch (and dump to file if
// logging diagnostics).
//
// Protagonists:
// metricsInfluxOutputHandler
//   dataChanRouter (one router feeding multiple workers)
//   metricsInfluxOutputWorker (multiple)
//
import (
	"bufio"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/influxdata/influxdb/client/v2"
	"os"
	"runtime"
	"time"
)

const (
	// Timeout waiting to enqueue message. If queues aren't drained in
	// this time (coupled with buf channel to absorb transients), then
	// we're toast.
	METRICS_INFLUX_TIMEOUT_ENQUEUE_SECONDS   = 2
	METRICS_INFLUX_WAIT_TO_RECONNECT_SECONDS = 2
	//
	// An estimate of fields per point allows us to setup
	// slices with a capacity which minimise reallocation,
	// without over allocating to omuch
	METRICS_INFLUX_FIELDS_PER_POINT_ESTIMATE = 32
)

type metricsInfluxOutputHandler struct {
	influxServer string
	database     string
	consistency  string
	retention    string
	standalone   bool
	auth         userPasswordCollector
	//
	// Workers are fed through a channel (of dataChannelDepth)
	// by the dataMsgRouter. lastWorker is used to track the
	// last worker used.
	workers          int
	router           *dataMsgRouter
	lastWorker       int
	dataChannelDepth int
	//
	// metricsfilename allow for diagnostic dump of metrics as
	// exported to InfluxDB
	metricsfilename string
	//
	// Logging context, built once, and reused.
	logctx *log.Entry
}

//
// metricsInfluxOutputWorker handles (sub)set of events and translates
// them into measurement POSTs to Influxdb, working completely
// autonomously from any other workers if present.
type metricsInfluxOutputWorker struct {
	influxServer        string
	wkid                int
	influxOutputHandler *metricsInfluxOutputHandler
	dataChan            chan dataMsg
	metricsfilename     string
	logctx              *log.Entry
}

type metricsInfluxOutputContext struct {
	name   string
	fields []*metricsAtom
	bp     client.BatchPoints
}

func (w *metricsInfluxOutputWorker) worker(m *metricsOutputModule) {

	var metricsfile *os.File
	var error_tag string
	var dump *bufio.Writer
	var influxClient client.Client
	var metadata *dataMsgMetaData
	var err error
	var bp client.BatchPoints

	w.logctx.Debug("Run worker")

	defer m.shutdownSyncPoint.Done()

	if w.metricsfilename != "" {
		metricsfile, dump = metricsSetupDumpfile(
			w.metricsfilename, w.logctx)
		if metricsfile != nil {
			defer metricsfile.Close()
		}
	}

	outputHandler := w.influxOutputHandler
	// Add the client batch configuration once, and reuse it
	// for every batch we add.
	batchCfg := client.BatchPointsConfig{
		Database:         outputHandler.database,
		Precision:        "ms",
		RetentionPolicy:  outputHandler.retention,
		WriteConsistency: outputHandler.consistency,
	}

	for {
		// Any failure, other than explicitly closed channels
		// indicating we're shutting down, causes us to go back to go,
		// collect £200 and try again.
		//
		// Add tls config here.
		if !outputHandler.standalone {
			var user, passw string
			user, passw, err = outputHandler.auth.getUP()
			if err == nil {
				influxClient, err = client.NewHTTPClient(client.HTTPConfig{
					Addr: w.influxServer, Username: user, Password: passw,
				})
			}
		}

		if err != nil {
			// Wait, and come round again
			w.logctx.WithError(err).Error("connect to influx node (will retry)")
			time.Sleep(
				time.Duration(METRICS_INFLUX_WAIT_TO_RECONNECT_SECONDS) * time.Second)
			continue
		}

		if outputHandler.standalone {
			w.logctx.Debug("Running in standalone mode (dumping points to file)")
		} else {
			w.logctx.Debug("Connected to influx node")
		}

		// Ok, we connected. Lets get into the main loop.
		for {
			msg, ok := <-w.dataChan
			if !ok {
				w.logctx.Debug("Closed worker")
				if !outputHandler.standalone {
					influxClient.Close()
				}
				return
			}

			bp, err = client.NewBatchPoints(batchCfg)
			if err != nil {
				// Break out of the loop and start worker over. We
				// failed to get a new batch.
				error_tag = "failed to create batch point"
				break
			}

			metadata = msg.getMetaData()
			context := &metricsInfluxOutputContext{
				name:   metadata.Path,
				fields: nil,
				bp:     bp,
			}
			err = msg.produceMetrics(&m.inputSpec, m.outputHandler, context)
			if err != nil {
				metricsStatMonitor.basePathMetricsError.WithLabelValues(
					m.name, metadata.Identifier, metadata.Path,
					"produce failed").Inc()
				continue
			}

			// If no metrics produced - perfectly valid, continue
			pts := bp.Points()
			if len(pts) == 0 {
				continue
			}

			// Dump metrics if doing diagnostics. Costly, of course.
			if dump != nil {
				dump.WriteString(fmt.Sprintf(
					"Server: [%s], wkid %d, writing %d points in db: %s\n"+
						"(prec: [%s], consistency: [%s], retention: [%s])\n",
					w.influxServer,
					w.wkid,
					len(pts),
					bp.Database(),
					bp.Precision(),
					bp.WriteConsistency(),
					bp.RetentionPolicy()))
				for _, pt := range pts {
					//
					// Start with a simple dump. Might need to extend a little.
					dump.WriteString(fmt.Sprintf("\t%s\n", pt.String()))
				}
			}

			if !outputHandler.standalone {
				err = influxClient.Write(context.bp)
			}
			if err != nil {
				error_tag = "failed to write batch point"
				break
			}
		}

		//
		// We would be here on error.
		metricsStatMonitor.basePathMetricsError.WithLabelValues(
			m.name, metadata.Identifier, metadata.Path,
			error_tag).Inc()

		//
		// Close existing client.
		if !outputHandler.standalone {
			influxClient.Close()
		}

		//
		// It might be too noisy to log error here. We may need to
		// consider dampening and relying on exported metric
		w.logctx.WithError(err).WithFields(log.Fields{
			"error_tag": error_tag}).Error(
			"exit loop handling messages, will reconnect")
		time.Sleep(
			time.Duration(METRICS_INFLUX_WAIT_TO_RECONNECT_SECONDS) * time.Second)

	}
}

func (o *metricsInfluxOutputHandler) setupWorkers(m *metricsOutputModule) {
	//
	// Setup as many workers with their own context as necessary. We
	// also route to the various workers using a dedicated
	// router. This will be generalised.
	//
	// Assuming we are picking up off the pub/sub bus, we could be
	// splitting the load by kafka partition already, and this
	// might be one instance of a group of pipelines operating as
	// a consumer group.
	//
	var dumpfilename string

	o.logctx.Info("Setting up workers")

	o.router = &dataMsgRouter{
		dataChanIn:   m.dataChan,
		shutdownChan: m.shutdownChan,
		dataChansOut: make([]chan dataMsg, o.workers),
		logctx:       o.logctx,
		route: func(msg dataMsg, attempts int) int {
			//
			// We start with simple round robin algorithm.
			o.lastWorker++
			o.lastWorker %= o.workers
			return o.lastWorker
		},
		handleCongested: func(
			msg dataMsg, attempts int, worker int) dataMsgRouterCongestionAction {

			metadata := msg.getMetaData()

			// Reroute to another worker.
			if attempts < o.workers {
				metricsStatMonitor.basePathMetricsError.WithLabelValues(
					m.name, metadata.Identifier, metadata.Path,
					"congested worker (rerouted)").Inc()
				return DATAMSG_ROUTER_REROUTE
			}
			//
			// Give up and drop.
			metricsStatMonitor.basePathMetricsError.WithLabelValues(
				m.name, metadata.Identifier, metadata.Path,
				"congested worker (dropped)").Inc()
			return DATAMSG_ROUTER_DROP
		},
		// We do not really use the timeout. Behaviour is currently to
		// hunt for worker whcih can take message or drop.
		timeout: time.Duration(METRICS_INFLUX_TIMEOUT_ENQUEUE_SECONDS) * time.Second,
	}

	//
	// Inherit channel depth for workers too.
	o.dataChannelDepth = m.dataChannelDepth
	for i := 0; i < o.workers; i++ {

		o.router.dataChansOut[i] = make(chan dataMsg, o.dataChannelDepth)

		m.shutdownSyncPoint.Add(1)
		if o.metricsfilename != "" {
			dumpfilename = fmt.Sprintf(
				"%s_wkid%d", o.metricsfilename, i)
		} else {
			dumpfilename = ""
		}

		w := &metricsInfluxOutputWorker{
			influxServer:        o.influxServer,
			wkid:                i,
			influxOutputHandler: o,
			dataChan:            o.router.dataChansOut[i],
			metricsfilename:     dumpfilename,
			logctx:              o.logctx.WithFields(log.Fields{"wkid": i}),
		}

		go w.worker(m)
	}

	//
	// Kick off router to start collecting and routing messages to
	// workers.
	go o.router.run()
}

//
// Simply colllect the metric atoms at this stage for Influx. We use
// the flush to assemble a new point, and clear the list.
func (o *metricsInfluxOutputHandler) buildMetric(
	tags []metricsAtom, sensor metricsAtom, ts uint64,
	context metricsOutputContext) {

	c := context.(*metricsInfluxOutputContext)

	if c.fields == nil {
		c.fields = make([]*metricsAtom, 0, METRICS_INFLUX_FIELDS_PER_POINT_ESTIMATE)
	}

	//
	// Rewrite accursed uint64:
	//
	//	fmt.Printf(" uint64 [%v] -> float64 [%v]\n",
	//   uint64(math.MaxUint64),
	//   float64(math.MaxUint64))
	//
	//  uint64 [18446744073709551615] -> float64 [1.8446744073709552e+19]
	//
	switch sensor.val.(type) {
	case uint64:
		sensor.val = float64(sensor.val.(uint64))
	default:
		// nothing to do!
	}

	c.fields = append(c.fields, &sensor)
}

func (o *metricsInfluxOutputHandler) flushMetric(
	tag_atoms []metricsAtom, ts uint64, context metricsOutputContext) {

	c := context.(*metricsInfluxOutputContext)

	if c.fields != nil {

		fields := map[string]interface{}{}
		for _, field_atom := range c.fields {
			fields[field_atom.key] = field_atom.val
		}

		tags := map[string]string{}
		for _, tag_atom := range tag_atoms {
			tags[tag_atom.key] = fmt.Sprintf("%v", tag_atom.val)
		}

		pt, err := client.NewPoint(c.name, tags, fields,
			time.Unix(0, int64(ts)*1000*1000))
		if err == nil {
			c.bp.AddPoint(pt)
		} else {
			//
			// Unexpected failure.
			o.logctx.WithFields(
				log.Fields{"base_path": c.name}).WithError(err).Error(
				"adding point to batch")
		}

		//
		// Finish by clearing the c.fields
		c.fields = nil
	}
}

func (o *metricsInfluxOutputHandler) adaptSensorName(name string) string {
	return name
}

func (o *metricsInfluxOutputHandler) adaptTagName(name string) string {
	return name
}

//
// Process the configuration to extract whatever is needed.
func metricsInfluxNew(name string, nc nodeConfig) (metricsOutputHandler, error) {

	var err error
	var authCollect userPasswordCollector
	var standalone bool

	logctx := logger.WithFields(log.Fields{
		"name":       name,
		"xport_type": "influx",
	})

	// If not set, will default to false
	metricsfilename, _ := nc.config.GetString(name, "dump")
	influxServer, _ := nc.config.GetString(name, "influx")
	if influxServer == "" {
		if metricsfilename == "" {
			err = fmt.Errorf(
				"attribute 'influx' required for influx metric export. " +
					"Specify URL of the form " +
					"http://[ipv6-host%%zone]:port or " +
					"http://influx.example.com:port. " +
					"Alternatively specify 'dump' option to just log points.")
			logctx.WithError(err).Error("insufficient configuration")
			return nil, err
		} else {
			standalone = true
		}
	} else {
		logctx = logctx.WithFields(log.Fields{"influx": influxServer})
	}

	//
	// TODO: Add TLS support by pulling in TLS config at this point.

	database, err := nc.config.GetString(name, "database")
	if err != nil {
		logctx.WithError(err).Error(
			"attribute 'database' required for influx metric export. " +
				" For a database created with 'CREATE DATABASE <name>', " +
				"this setting would be 'database=<name>'")
		return nil, err
	}
	logctx = logctx.WithFields(log.Fields{"database": database})

	// Handle user/password
	if !standalone {
		authCollect = influxUPCollectorFactory()
		err = authCollect.handleConfig(nc, name, influxServer)
		if err != nil {
			logctx.WithError(err).Error(
				"failed to collect credentials required for influx")
			return nil, err
		}
	}

	//
	// One could imagine templatising these in exactly the same way as
	// we templatise deriving topic in kafka. Future.
	consistency, _ := nc.config.GetString(name, "consistency")
	retention, _ := nc.config.GetString(name, "retention")

	workers, _ := nc.config.GetInt(name, "workers")
	if workers == 0 {
		workers = 1
	} else if workers > runtime.GOMAXPROCS(-1) {
		//
		// Excessive number of workers... cut back
		workers = runtime.GOMAXPROCS(-1)
	}
	logctx = logctx.WithFields(log.Fields{"workers": workers})

	//
	// Populate the influx output context
	out := &metricsInfluxOutputHandler{
		influxServer:    influxServer,
		auth:            authCollect,
		database:        database,
		consistency:     consistency,
		retention:       retention,
		workers:         workers,
		standalone:      standalone,
		logctx:          logctx,
		metricsfilename: metricsfilename,
	}

	return out, nil
}

var influxUPCollectorFactory userPasswordCollectorFactory

//
// We use init to setup the user/password collection function. This
// can be overwritten by test.
func init() {

	influxUPCollectorFactory = func() userPasswordCollector {
		return &cryptUserPasswordCollector{}
	}
}
