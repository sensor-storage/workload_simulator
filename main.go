package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"os"
	"os/signal"
	"time"
)

func main() {
	logger := logrus.New()
	logger.Out = os.Stdout
	configFilename := flag.String("f", "config.yaml", "Filename of configuration YAML file")
	testTime := flag.Duration("t", 300*time.Second, "Max test time (in go format)")
	flag.Parse()
	cancelCtx, cancel := context.WithTimeout(context.Background(), *testTime)

	config, err := LoadConfigFile(*configFilename)

	if err != nil {
		logger.WithFields(logrus.Fields{
			"event": "Config file not found",
		}).Fatal()
		return
	}

	totalWorkers := 0
	for _, workerTemplate := range config.Workload.WorkerTemplates {
		totalWorkers += workerTemplate.Workers
	}

	workers := make([]*Worker, 0, totalWorkers)

	for _, workerTemplate := range config.Workload.WorkerTemplates {
		logger.WithFields(logrus.Fields{
			"event": fmt.Sprintf("Initializing workers for %s", workerTemplate.Measurement),
		}).Info()
		bar := pb.New(workerTemplate.Workers)
		bar.Start()
		for i := 0; i < workerTemplate.Workers; i++ {
			workers = append(workers, NewWorker(cancelCtx, config.InfluxDB, workerTemplate, i))
			bar.Increment()
		}
		bar.Finish()
	}

	logger.WithFields(logrus.Fields{
		"event": "Starting Workers",
	}).Info()

	sigintChan := make(chan os.Signal)
	signal.Notify(sigintChan, unix.SIGINT)

	go func() {
		<-sigintChan
		cancel()
	}()

	startTime := time.Now()
	done := false
	for _, worker := range workers {
		select {
		case <-cancelCtx.Done():
			done = true
		default:
			go worker.DoWork(logger, config.Workload.LogPerWorks)
		}
		if done {
			break
		}
	}

	if !done {
		<-cancelCtx.Done()
	}
	endTime := time.Now()
	passedDuration := endTime.Sub(startTime)
	totalWorks := 0
	for _, worker := range workers {
		totalWorks += worker.WorksDone
	}
	throughput := float64(totalWorks) / float64(passedDuration/time.Second)
	logger.WithFields(logrus.Fields{
		"event":      "Test Done",
		"totalTime":  fmt.Sprint(passedDuration),
		"throughput": fmt.Sprintf("%f works/s", throughput),
	}).Info()
}
