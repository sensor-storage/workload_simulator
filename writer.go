package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/sirupsen/logrus"
)

type metric struct {
	measurement string
	tags        []string
	fields      []string
}

func makeMetrics(writerName string) []metric {
	//measurements: 液位/水泵状态/水泵运行频率/水压/水硬度/电导率
	var measurements = []string{"liquid_level", "pump_condition", "pump_frequency", "water_pressure", "water_hardness", "conductivity"}
	//tag(组件部位): 原水罐/软化器/RO/EDI
	var tags = []string{"raw_water_tank", "Softener", "RO", "EDI"}
	//field(组件测点): 进水口/出水口
	var examplefields = []string{"inlet", "outlet"}
	configArray := [6][4]int{
		{1, 0, 0, 0},
		{1, 1, 1, 1},
		{1, 0, 1, 1},
		{1, 1, 2, 2}, //仅2时应用field
		{0, 1, 0, 0},
		{0, 0, 1, 1},
	}
	var metrics = []metric{}
	for i := 0; i < 6; i++ {
		for j := 0; j < 4; j++ {
			if configArray[i][j] == 1 {
				tag := []string{"writer=" + writerName, "component=" + tags[j]}
				field := []string{measurements[i]}
				metrics = append(metrics, metric{measurement: measurements[i], tags: tag, fields: field})
			}
			if configArray[i][j] == 2 {
				tag := []string{"writer=" + writerName, "component=" + tags[j]}
				metrics = append(metrics, metric{measurement: measurements[i], tags: tag, fields: examplefields})
			}
		}
	}
	return metrics
}

//Writer is ...
type Writer struct {
	Worker
	writeAPI     api.WriteAPI
	metrics      []metric
	emitInterval time.Duration
	batchSize    uint
}

//NewWriter is ...
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
		metrics:      makeMetrics(name),
		emitInterval: template.EmitInterval,
		batchSize:    template.BatchSize,
	}

	return worker
}

func (w *Writer) emitValue() {
	var timeNow = time.Now()
	id, err := strconv.Atoi(w.name[(strings.Index(w.name, "_") + 1):])
	if err != nil {
		fmt.Println("字符串转换成整数失败")
	}
	for _, oneMetric := range w.metrics {
		point := influxdb2.NewPointWithMeasurement(oneMetric.measurement)

		for _, tag := range oneMetric.tags {
			segs := strings.Split(tag, "=")
			point.AddTag(segs[0], segs[1])
		}

		for _, field := range oneMetric.fields {
			point.AddField(field, rand.Float64()+float64(id))
		}

		point.SetTime(timeNow)
		w.writeAPI.WritePoint(point)
	}
}

//DoWork is ...
func (w *Writer) DoWork(logger *logrus.Logger, logPerWorks uint) {
	var collectInterval = time.Duration(int64(float64(w.emitInterval.Milliseconds())/(float64(w.batchSize)/float64(len(w.metrics))))) * time.Millisecond
	var collectPointNumber = uint(len(w.metrics))
	for true {
		select {
		case <-w.cancelCtx.Done():
			return
		default:
			// for i := uint(0); i < w.batchSize; i++ {
			// 	w.emitValue()
			// }

			// w.WorksDone += w.batchSize
			// if w.WorksDone%logPerWorks == 0 {
			// 	logger.WithFields(logrus.Fields{
			// 		"name":      w.name,
			// 		"worksDone": w.WorksDone,
			// 	}).Info()
			// }

			go func() {
				w.emitValue()

				w.WorksDone += collectPointNumber
				if w.WorksDone%logPerWorks == 0 {
					logger.WithFields(logrus.Fields{
						"name":      w.name,
						"worksDone": w.WorksDone,
					}).Info()
				}
			}()

			time.Sleep(collectInterval)
		}
	}
}
