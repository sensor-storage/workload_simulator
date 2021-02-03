package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/sirupsen/logrus"
)

//WriterWithoutBatch define writer which emit important information as fastly as possible
type WriterWithoutBatch struct {
	Worker
	writeAPI     api.WriteAPIBlocking
	measurement  string
	tags         []string
	fields       []string
	emitInterval time.Duration
	Latencies    []time.Duration
}

//NewWriterWithoutBatch is for writerWithoutBatch
func NewWriterWithoutBatch(cancelCtx context.Context, db InfluxDBConfig, template WriterTemplate, id int) *WriterWithoutBatch {
	client := influxdb2.NewClient(db.Host, db.Token)
	name := fmt.Sprintf("%s_%d", template.Measurement, id)
	writerWithoutBatch := &WriterWithoutBatch{
		Worker: Worker{
			cancelCtx: cancelCtx,
			client:    client,
			name:      name,
		},
		writeAPI:     client.WriteAPIBlocking(db.Organization, db.Bucket),
		measurement:  template.Measurement,
		tags:         template.Tags,
		fields:       template.Fields,
		emitInterval: template.EmitInterval,
		Latencies:    make([]time.Duration, 0),
	}

	return writerWithoutBatch
}

func (w *WriterWithoutBatch) emitValue() error {
	point := influxdb2.NewPointWithMeasurement(w.measurement)

	id, errAtoi := strconv.Atoi(w.name[(strings.Index(w.name, "_") + 1):])
	if errAtoi != nil {
		fmt.Println("字符串转换成整数失败")
	}

	point.AddTag("writer", w.name)
	for _, tag := range w.tags {
		segs := strings.Split(tag, "=")
		point.AddTag(segs[0], segs[1])
	}

	for _, field := range w.fields {
		point.AddField(field, rand.Float64()+float64(id))
	}

	point.SetTime(time.Now())

	startTime := time.Now()
	err := w.writeAPI.WritePoint(w.cancelCtx, point)
	endTime := time.Now()
	w.Latencies = append(w.Latencies, endTime.Sub(startTime))
	return err
}

//DoWork Without Batch
func (w *WriterWithoutBatch) DoWork(logger *logrus.Logger, logPerWorks uint) {
	for true {
		select {
		case <-w.cancelCtx.Done():
			return
		default:
			go func() {
				err := w.emitValue()
				if err != nil {
					logger.WithFields(logrus.Fields{
						"name":  w.name,
						"error": fmt.Sprintf("%v", err),
					}).Fatal()
				}

				w.WorksDone++
				if w.WorksDone%logPerWorks == 0 {
					logger.WithFields(logrus.Fields{
						"name":      w.name,
						"worksDone": w.WorksDone,
						"goroutine": runtime.NumGoroutine(),
					}).Info()
				}
			}()

			time.Sleep(w.emitInterval)
		}
	}
}

//CollectLatencies is ...
func (w *WriterWithoutBatch) CollectLatencies() (time.Duration, time.Duration, time.Duration) {
	sort.Slice(w.Latencies, func(i, j int) bool {
		return w.Latencies[i] < w.Latencies[j]
	})
	numLatencies := len(w.Latencies)
	fmt.Println(numLatencies)
	index90Percent := uint(math.Ceil(float64(numLatencies)*0.9)) - 1
	index95Percent := uint(math.Ceil(float64(numLatencies)*0.95)) - 1
	index99Percent := uint(math.Ceil(float64(numLatencies)*0.99)) - 1
	return w.Latencies[index90Percent], w.Latencies[index95Percent], w.Latencies[index99Percent]
}
