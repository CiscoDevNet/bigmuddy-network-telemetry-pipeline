// Command pipeline collects network streaming telemetry (gNMI, Cisco MDT)
// and delivers it to Prometheus, OpenTelemetry, InfluxDB, Kafka or files.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	versioncollector "github.com/prometheus/client_golang/prometheus/collectors/version"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/admin"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/otlp"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"

	// Plugins register themselves.
	_ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/input/ciscomdt"
	_ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/input/gnmi"
	_ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/file"
	_ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/influxdb"
	_ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/kafka"
	_ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/prometheus"
	_ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/processor"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], nil)
	stop()
	os.Exit(code)
}

// run is main without process-global side effects, so it can be tested.
// started, if non-nil, receives the admin address once serving.
func run(ctx context.Context, args []string, started chan<- string) int {
	fs := flag.NewFlagSet("pipeline", flag.ContinueOnError)
	cfgPath := fs.String("config", "pipeline.yaml", "path to the YAML configuration file")
	check := fs.Bool("check", false, "validate the configuration (including building every plugin) and exit")
	showVersion := fs.Bool("version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Println(version)
		return 0
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		return 1
	}
	log := newLogger(cfg.Log)
	slog.SetDefault(log)
	otlp.Version = version

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		versioncollector.NewCollector("pipeline"))

	engine, err := pipeline.New(cfg, log, reg)
	if err != nil {
		log.Error("invalid configuration", "err", err)
		return 1
	}
	if *check {
		fmt.Println("configuration OK")
		return 0
	}

	if cfg.Admin.Listen != "" {
		adm, err := admin.New(cfg.Admin.Listen, cfg.Admin.Pprof, reg, engine.Ready, log)
		if err != nil {
			log.Error("admin endpoint", "err", err)
			return 1
		}
		adm.Start()
		if started != nil {
			started <- adm.Addr().String()
		}
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			adm.Shutdown(sctx)
		}()
	}

	log.Info("starting pipeline", "version", version, "config", *cfgPath)
	if err := engine.Run(ctx); err != nil {
		log.Error("pipeline failed", "err", err)
		return 1
	}
	return 0
}

func newLogger(c config.Log) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(strings.ToUpper(c.Level))); err != nil {
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	if c.Format == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}
