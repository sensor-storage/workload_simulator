package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/sirupsen/logrus"
)

//Checker is ...
type Checker struct {
	Worker
	checkerConfig CheckerConfig
	queryAPI      api.QueryAPI
	query         string
	interval      time.Duration
	Latencies     []time.Duration
}

//NewChecker is ...
func NewChecker(cancelCtx context.Context, db InfluxDBConfig, config CheckerConfig) *Checker {
	client := influxdb2.NewClient(db.Host, db.Token)
	worker := &Checker{
		Worker: Worker{
			cancelCtx: cancelCtx,
			client:    client,
			name:      config.Name,
		},
		checkerConfig: config,
		queryAPI:      client.QueryAPI(db.Organization),
		query:         config.Query,
		interval:      config.Interval,
		Latencies:     make([]time.Duration, 0),
	}

	return worker
}

func (c *Checker) execQuery() error {
	res, err := c.queryAPI.Query(c.cancelCtx, c.query)
	if err != nil {
		return err
	}
	defer res.Close()

	for true {
		_ = res.Record()
		if !res.Next() {
			break
		}
	}

	return nil
}

//DoWork is ...
func (c *Checker) DoWork(logger *logrus.Logger, logPerWorks uint) {
	for true {
		select {
		case <-c.cancelCtx.Done():
			return
		default:
			startTime := time.Now()
			err := c.execQuery()
			if err != nil {
				logger.WithFields(logrus.Fields{
					"name":  c.name,
					"error": fmt.Sprintf("%v", err),
				}).Warn()
				return
			}
			endTime := time.Now()
			c.Latencies = append(c.Latencies, endTime.Sub(startTime))
			c.WorksDone++

			if c.WorksDone%logPerWorks == 0 {
				logger.WithFields(logrus.Fields{
					"name":      c.name,
					"worksDone": c.WorksDone,
				}).Info()
			}

			time.Sleep(c.interval)
		}
	}
}

//CollectLatencies is ...
func (c *Checker) CollectLatencies() (time.Duration, time.Duration, time.Duration) {
	sort.Slice(c.Latencies, func(i, j int) bool {
		return c.Latencies[i] < c.Latencies[j]
	})
	numLatencies := len(c.Latencies)
	fmt.Println(numLatencies)
	index90Percent := uint(math.Ceil(float64(numLatencies)*0.9)) - 1
	index95Percent := uint(math.Ceil(float64(numLatencies)*0.95)) - 1
	index99Percent := uint(math.Ceil(float64(numLatencies)*0.99)) - 1
	return c.Latencies[index90Percent], c.Latencies[index95Percent], c.Latencies[index99Percent]
}
