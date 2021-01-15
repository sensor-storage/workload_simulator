package main

import (
	"context"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/sirupsen/logrus"
	"math/rand"
	"strings"
	"time"
)

type Worker struct {
	cancelCtx    context.Context
	client       influxdb2.Client
	writeAPI     api.WriteAPIBlocking
	name         string
	measurement  string
	tags         []string
	fields       []string
	emitInterval int
	WorksDone    int
}

func NewWorker(cancelCtx context.Context, db InfluxDBConfig, template WorkerTemplate, id int) *Worker {
	client := influxdb2.NewClient(db.Host, db.Token)
	name := fmt.Sprintf("%s_%d", template.Measurement, id)
	worker := &Worker{
		cancelCtx:    cancelCtx,
		client:       client,
		writeAPI:     client.WriteAPIBlocking(db.Organization, db.Bucket),
		name:         name,
		measurement:  template.Measurement,
		tags:         template.Tags,
		fields:       template.Fields,
		emitInterval: template.EmitInterval,
	}

	return worker
}

func (w *Worker) emitValue() error {
	point := influxdb2.NewPointWithMeasurement(w.measurement)

	for _, tag := range w.tags {
		segs := strings.Split(tag, "=")
		point.AddTag(segs[0], segs[1])
	}

	for _, field := range w.fields {
		point.AddField(field, rand.Float64())
	}

	point.SetTime(time.Now())
	err := w.writeAPI.WritePoint(w.cancelCtx, point)
	return err
}

func (w *Worker) DoWork(logger *logrus.Logger, logPerWorks int) {
	for true {
		select {
		case <-w.cancelCtx.Done():
			return
		default:
			err := w.emitValue()
			if err != nil {
				logger.WithFields(logrus.Fields{
					"name":  w.name,
					"error": fmt.Sprintf("%v", err),
				}).Fatal()
			}

			w.WorksDone += 1
			if w.WorksDone%logPerWorks == 0 {
				logger.WithFields(logrus.Fields{
					"name":      w.name,
					"worksDone": w.WorksDone,
				}).Info()
			}

			noiseTime := time.Duration((rand.Float64()-0.5)*1000) * time.Millisecond
			time.Sleep(time.Duration(w.emitInterval)*time.Millisecond + noiseTime)
		}
	}
}
