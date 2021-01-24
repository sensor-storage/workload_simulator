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

type Writer struct {
	Worker
	writeAPI     api.WriteAPI
	measurement  string
	tags         []string
	fields       []string
	emitInterval time.Duration
	batchSize    uint
}

func NewWriter(cancelCtx context.Context, db InfluxDBConfig, template WriterTemplate, id int) *Writer {
	options := influxdb2.DefaultOptions().SetBatchSize(template.BatchSize)
	client := influxdb2.NewClientWithOptions(db.Host, db.Token, options)
	name := fmt.Sprintf("%s_%d", template.Measurement, id)
	worker := &Writer{
		Worker: Worker{
			cancelCtx: cancelCtx,
			client:    client,
			name:      name,
		},
		writeAPI:     client.WriteAPI(db.Organization, db.Bucket),
		measurement:  template.Measurement,
		tags:         template.Tags,
		fields:       template.Fields,
		emitInterval: template.EmitInterval,
		batchSize:    template.BatchSize,
	}

	return worker
}

func (w *Writer) emitValue() {
	point := influxdb2.NewPointWithMeasurement(w.measurement)

	for _, tag := range w.tags {
		segs := strings.Split(tag, "=")
		point.AddTag(segs[0], segs[1])
	}

	for _, field := range w.fields {
		point.AddField(field, rand.Float64())
	}

	point.SetTime(time.Now())
	w.writeAPI.WritePoint(point)
}

func (w *Writer) DoWork(logger *logrus.Logger, logPerWorks uint) {
	for true {
		select {
		case <-w.cancelCtx.Done():
			return
		default:
			for i := uint(0); i < w.batchSize; i++ {
				w.emitValue()
			}

			w.WorksDone += w.batchSize
			if w.WorksDone%logPerWorks == 0 {
				logger.WithFields(logrus.Fields{
					"name":      w.name,
					"worksDone": w.WorksDone,
				}).Info()
			}

			time.Sleep(w.emitInterval)
		}
	}
}
