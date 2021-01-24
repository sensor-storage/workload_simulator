package main

import (
	"context"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

type Worker struct {
	cancelCtx context.Context
	client    influxdb2.Client
	name      string
	WorksDone uint
}

func (w *Worker) GetName() string {
	return w.name
}
