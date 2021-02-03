package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

//Simulator is ...
type Simulator struct {
	logger              *logrus.Logger
	config              *Config
	cancelCtx           context.Context
	canceller           context.CancelFunc
	writers             []*Writer
	writerWithoutBatchs []*WriterWithoutBatch
	checkers            []*Checker
	startTime           time.Time
	endTime             time.Time
}

//NewSimulator is ...
func NewSimulator() *Simulator {
	return &Simulator{}
}

func (s *Simulator) init() error {
	logger := logrus.New()
	logger.Out = os.Stdout
	s.logger = logger

	configFilename := flag.String("f", "config.yaml", "Filename of configuration YAML file")
	testTime := flag.Duration("t", 30*time.Second, "Max test time (in go format)")
	flag.Parse()
	cancelCtx, cancel := context.WithTimeout(context.Background(), *testTime)

	config, err := LoadConfigFile(*configFilename)

	s.cancelCtx = cancelCtx
	s.canceller = cancel
	s.config = config

	if err != nil {
		logger.WithFields(logrus.Fields{
			"event": "Config file not found",
		}).Fatal()
		return err
	}

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

func (s *Simulator) createWriterWithoutBatchs() {
	totalWriterWithoutBatchs := 0
	for _, writerWithoutBatchTemplate := range s.config.Workload.WriterWithoutBatchTemplates {
		totalWriterWithoutBatchs += writerWithoutBatchTemplate.Workers
	}

	writerWithoutBatchs := make([]*WriterWithoutBatch, 0, totalWriterWithoutBatchs)

	for _, writerWithoutBatchTemplate := range s.config.Workload.WriterWithoutBatchTemplates {
		s.logger.WithFields(logrus.Fields{
			"event": fmt.Sprintf("Initializing writers for %s", writerWithoutBatchTemplate.Measurement),
		}).Info()
		bar := pb.New(writerWithoutBatchTemplate.Workers)
		bar.Start()
		for i := 0; i < writerWithoutBatchTemplate.Workers; i++ {
			writerWithoutBatchs = append(writerWithoutBatchs, NewWriterWithoutBatch(s.cancelCtx, s.config.InfluxDB, writerWithoutBatchTemplate, i))
			bar.Increment()
		}
		bar.Finish()
	}

	s.writerWithoutBatchs = writerWithoutBatchs
}

func (s *Simulator) createCheckers() {
	checkers := make([]*Checker, 0)

	for _, checkerConfig := range s.config.Workload.Checkers {
		s.logger.WithFields(logrus.Fields{
			"event": fmt.Sprintf("Initializing checker %s, number = %d", checkerConfig.Name, checkerConfig.CheckerNumber),
		}).Info()
		var number int = 1
		if checkerConfig.CheckerNumber != 0 {
			number = checkerConfig.CheckerNumber
		}
		bar := pb.New(number)
		bar.Start()
		for i := 0; i < number; i++ {
			checkers = append(checkers, NewChecker(s.cancelCtx, s.config.InfluxDB, checkerConfig))
			bar.Increment()
		}
		bar.Finish()
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

	for _, WriterWithoutBatch := range s.writerWithoutBatchs {
		go WriterWithoutBatch.DoWork(s.logger, s.config.Workload.LogPerWorks)
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
	for _, workerWithoutBatch := range s.writerWithoutBatchs {
		writeWorks += workerWithoutBatch.WorksDone
	}
	passedDuration := s.endTime.Sub(s.startTime)
	throughput := float64(writeWorks) / float64(passedDuration.Seconds())
	s.logger.WithFields(logrus.Fields{
		"event":           "Test Done",
		"totalTime":       fmt.Sprint(passedDuration),
		"writeThroughput": fmt.Sprintf("%f works/s", throughput),
	}).Info()

	s.logger.WithFields(logrus.Fields{
		"event": "Show writer without batch's emit duration as below (共20个，只显示前5个）",
	}).Info()
	for index, writerWithoutBatch := range s.writerWithoutBatchs {
		lat90Pct, lat95Pct, lat99Pct := writerWithoutBatch.CollectLatencies()
		s.logger.WithFields(logrus.Fields{
			"name":        writerWithoutBatch.GetName(),
			"90% Latency": fmt.Sprintf("%s", lat90Pct),
			"95% Latency": fmt.Sprintf("%s", lat95Pct),
			"99% Latency": fmt.Sprintf("%s", lat99Pct),
		}).Info()
		if index >= 4 {
			break
		}
	}

	s.logger.WithFields(logrus.Fields{
		"event": "Show checker duration as below",
	}).Info()
	for i := 0; i < len(s.checkers); i++ {
		var checker = s.checkers[i]
		var number int = 1
		if checker.checkerConfig.CheckerNumber != 0 {
			number = checker.checkerConfig.CheckerNumber
		}
		var totalLat90Pct time.Duration = 0
		var totalLat95Pct time.Duration = 0
		var totalLat99Pct time.Duration = 0
		for j := 0; j < number; j++ {
			lat90Pct, lat95Pct, lat99Pct := s.checkers[i+j].CollectLatencies()
			totalLat90Pct += lat90Pct
			totalLat95Pct += lat95Pct
			totalLat99Pct += lat99Pct
		}
		totalLat90Pct = time.Duration(float64(totalLat90Pct.Milliseconds())/float64(number)) * time.Millisecond
		totalLat95Pct = time.Duration(float64(totalLat95Pct.Milliseconds())/float64(number)) * time.Millisecond
		totalLat99Pct = time.Duration(float64(totalLat99Pct.Milliseconds())/float64(number)) * time.Millisecond
		s.logger.WithFields(logrus.Fields{
			"name":        checker.GetName(),
			"90% Latency": fmt.Sprintf("%s", totalLat90Pct),
			"95% Latency": fmt.Sprintf("%s", totalLat95Pct),
			"99% Latency": fmt.Sprintf("%s", totalLat99Pct),
			"numbers":     number,
		}).Info()
		i += number - 1
	}
	// for _, checker := range s.checkers {
	// 	lat90Pct, lat95Pct, lat99Pct := checker.CollectLatencies()
	// 	s.logger.WithFields(logrus.Fields{
	// 		"name":        checker.GetName(),
	// 		"90% Latency": fmt.Sprintf("%s", lat90Pct),
	// 		"95% Latency": fmt.Sprintf("%s", lat95Pct),
	// 		"99% Latency": fmt.Sprintf("%s", lat99Pct),
	// 	}).Info()
	// }
}

//Run is ...
func (s *Simulator) Run() {
	err := s.init()
	if err != nil {
		s.logger.WithFields(logrus.Fields{
			"error": fmt.Sprintf("%v", err),
		}).Fatal()
		return
	}

	s.createWriters()
	s.createWriterWithoutBatchs()
	s.createCheckers()

	s.startSignalListener()
	s.startWorkers()

	s.wait()
	s.report()
}
