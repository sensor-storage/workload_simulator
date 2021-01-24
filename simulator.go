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

type Simulator struct {
	logger    *logrus.Logger
	config    *Config
	cancelCtx context.Context
	canceller context.CancelFunc
	writers   []*Writer
	checkers  []*Checker
	startTime time.Time
	endTime   time.Time
}

func NewSimulator() *Simulator {
	return &Simulator{}
}

func (s *Simulator) init() error {
	logger := logrus.New()
	logger.Out = os.Stdout
	s.logger = logger

	configFilename := flag.String("f", "config.yaml", "Filename of configuration YAML file")
	testTime := flag.Duration("t", 300*time.Second, "Max test time (in go format)")
	flag.Parse()
	cancelCtx, cancel := context.WithTimeout(context.Background(), *testTime)

	config, err := LoadConfigFile(*configFilename)

	if err != nil {
		logger.WithFields(logrus.Fields{
			"event": "Config file not found",
		}).Fatal()
		return err
	}

	s.cancelCtx = cancelCtx
	s.canceller = cancel
	s.config = config

	return nil
}

func (s *Simulator) createWriters() {
	totalWriters := 0
	for _, writerTemplate := range s.config.Workload.WriterTemplates {
		totalWriters += writerTemplate.Workers
	}

	writers := make([]*Writer, 0, totalWriters)

	for _, writerTemplate := range s.config.Workload.WriterTemplates {
		s.logger.WithFields(logrus.Fields{
			"event": fmt.Sprintf("Initializing writers for %s", writerTemplate.Measurement),
		}).Info()
		bar := pb.New(writerTemplate.Workers)
		bar.Start()
		for i := 0; i < writerTemplate.Workers; i++ {
			writers = append(writers, NewWriter(s.cancelCtx, s.config.InfluxDB, writerTemplate, i))
			bar.Increment()
		}
		bar.Finish()
	}

	s.writers = writers
}

func (s *Simulator) createCheckers() {
	checkers := make([]*Checker, 0)

	for _, checkerConfig := range s.config.Workload.Checkers {
		s.logger.WithFields(logrus.Fields{
			"event": fmt.Sprintf("Initializing checker %s", checkerConfig.Name),
		}).Info()
		checkers = append(checkers, NewChecker(s.cancelCtx, s.config.InfluxDB, checkerConfig))
	}

	s.checkers = checkers
}

func (s *Simulator) startSignalListener() {
	sigintChan := make(chan os.Signal)
	signal.Notify(sigintChan, unix.SIGINT)

	go func() {
		<-sigintChan
		s.canceller()
	}()
}

func (s *Simulator) startWorkers() {
	s.logger.WithFields(logrus.Fields{
		"event": "Starting Workers",
	}).Info()
	s.startTime = time.Now()
	for _, writer := range s.writers {
		go writer.DoWork(s.logger, s.config.Workload.LogPerWorks)
	}

	for _, checker := range s.checkers {
		go checker.DoWork(s.logger, s.config.Workload.LogPerWorks)
	}
}

func (s *Simulator) wait() {
	<-s.cancelCtx.Done()
	s.endTime = time.Now()
}

func (s *Simulator) report() {
	writeWorks := uint(0)
	for _, worker := range s.writers {
		writeWorks += worker.WorksDone
	}
	passedDuration := s.endTime.Sub(s.startTime)
	throughput := float64(writeWorks) / passedDuration.Seconds()
	s.logger.WithFields(logrus.Fields{
		"event":           "Test Done",
		"totalTime":       fmt.Sprint(passedDuration),
		"writeThroughput": fmt.Sprintf("%f works/s", throughput),
	}).Info()

	for _, checker := range s.checkers {
		lat90Pct, lat95Pct, lat99Pct := checker.CollectLatencies()
		s.logger.WithFields(logrus.Fields{
			"name":        checker.GetName(),
			"90% Latency": fmt.Sprintf("%s", lat90Pct),
			"95% Latency": fmt.Sprintf("%s", lat95Pct),
			"99% Latency": fmt.Sprintf("%s", lat99Pct),
		}).Info()
	}
}

func (s *Simulator) Run() {
	err := s.init()
	if err != nil {
		s.logger.WithFields(logrus.Fields{
			"error": fmt.Sprintf("%v", err),
		}).Fatal()
		return
	}

	s.createWriters()
	s.createCheckers()

	s.startSignalListener()
	s.startWorkers()

	s.wait()
	s.report()
}
