package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const exampleConfig = `
influxDB:
  host: 127.0.0.1
  token: testtoken
  organization: my-org
  bucket: my-bucket
workload:
  logPerWorks: 100
  writerTemplates:
    - measurement: temperature
      tags:
        - test1=test1
        - test2=test2
      fields:
        - census
      workers: 10
      emitInterval: 0.1s
      batchSize: 1000
    - measurement: luminance
      tags:
        - test3=test3
        - test4=test4
      fields:
        - lumen
      workers: 10
      emitInterval: 0.2s
      batchSize: 1000
  checkers:
    - query: |
        from(bucket: "...")
        |> range(start: -10m)
        |> aggregateWindow(every: 1m, fn: max)
      interval: 10s
      name: aaa

`

var expectTemplates = []WriterTemplate{
	{
		Measurement:  "temperature",
		Tags:         []string{"test1=test1", "test2=test2"},
		Fields:       []string{"census"},
		Workers:      10,
		EmitInterval: 100 * time.Millisecond,
		BatchSize:    1000,
	},
	{
		Measurement:  "luminance",
		Tags:         []string{"test3=test3", "test4=test4"},
		Fields:       []string{"lumen"},
		Workers:      10,
		EmitInterval: 200 * time.Millisecond,
		BatchSize:    1000,
	},
}

var expectedCheckers = []CheckerConfig{
	{
		Query: `from(bucket: "...")
|> range(start: -10m)
|> aggregateWindow(every: 1m, fn: max)
`,
		Interval: 10 * time.Second,
		Name:     "aaa",
	},
}

func assertWriterTemplateEqual(t *testing.T, expected *WriterTemplate, actual *WriterTemplate) {
	assert.Equal(t, expected.Workers, actual.Workers)
	assert.Equal(t, expected.Measurement, actual.Measurement)
	assert.Equal(t, expected.EmitInterval, actual.EmitInterval)
	assert.Equal(t, expected.BatchSize, actual.BatchSize)

	for i, field := range expected.Fields {
		assert.Equal(t, field, actual.Fields[i])
	}

	for i, tag := range expected.Tags {
		assert.Equal(t, tag, actual.Tags[i])
	}
}

func assertCheckerEqual(t *testing.T, expected *CheckerConfig, actual *CheckerConfig) {
	assert.Equal(t, expected.Query, actual.Query)
	assert.Equal(t, expected.Interval, actual.Interval)
	assert.Equal(t, expected.Name, actual.Name)
}

func assertExampleConfig(t *testing.T, actual *Config) {
	assert.Equal(t, "127.0.0.1", actual.InfluxDB.Host)
	assert.Equal(t, "testtoken", actual.InfluxDB.Token)
	assert.Equal(t, "my-org", actual.InfluxDB.Organization)
	assert.Equal(t, "my-bucket", actual.InfluxDB.Bucket)

	assert.Equal(t, uint(100), actual.Workload.LogPerWorks)

	for i, writerTemplate := range actual.Workload.WriterTemplates {
		assertWriterTemplateEqual(t, &expectTemplates[i], &writerTemplate)
	}

	for i, checkerConfig := range actual.Workload.Checkers {
		assertCheckerEqual(t, &expectedCheckers[i], &checkerConfig)
	}
}

func TestParseConfig(t *testing.T) {
	config, err := ParseConfig([]byte(exampleConfig))
	assert.Nil(t, err)

	assertExampleConfig(t, config)
}

func TestLoadConfigFile(t *testing.T) {
	config, err := LoadConfigFile("config_example.yaml")
	assert.Nil(t, err)

	assertExampleConfig(t, config)
}
