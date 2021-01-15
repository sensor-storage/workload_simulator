package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const exampleConfig = `
influxDB:
  host: 127.0.0.1
  token: testtoken
  organization: my-org
  bucket: my-bucket
workload:
  logPerWorks: 100
  workerTemplates:
    - measurement: temperature
      tags:
        - test1=test1
        - test2=test2
      fields:
        - census
      workers: 1000
      emitInterval: 1000
    - measurement: luminance
      tags:
        - test3=test3
        - test4=test4
      fields:
        - lumen
      workers: 1000
      emitInterval: 2000
`

var expectTemplates = []WorkerTemplate{
	{
		Measurement:  "temperature",
		Tags:         []string{"test1=test1", "test2=test2"},
		Fields:       []string{"census"},
		Workers:      1000,
		EmitInterval: 1000,
	},
	{
		Measurement:  "luminance",
		Tags:         []string{"test3=test3", "test4=test4"},
		Fields:       []string{"lumen"},
		Workers:      1000,
		EmitInterval: 2000,
	},
}

func assertWorkerTemplateEqual(t *testing.T, expected *WorkerTemplate, actual *WorkerTemplate) {
	assert.Equal(t, expected.Workers, actual.Workers)
	assert.Equal(t, expected.Measurement, actual.Measurement)
	assert.Equal(t, actual.EmitInterval, expected.EmitInterval)

	for i, field := range expected.Fields {
		assert.Equal(t, field, actual.Fields[i])
	}

	for i, tag := range expected.Tags {
		assert.Equal(t, tag, actual.Tags[i])
	}
}

func assertExampleConfig(t *testing.T, actual *Config) {
	assert.Equal(t, "127.0.0.1", actual.InfluxDB.Host)
	assert.Equal(t, "testtoken", actual.InfluxDB.Token)
	assert.Equal(t, "my-org", actual.InfluxDB.Organization)
	assert.Equal(t, "my-bucket", actual.InfluxDB.Bucket)

	assert.Equal(t, 100, actual.Workload.LogPerWorks)

	for i, workerTemplate := range actual.Workload.WorkerTemplates {
		assertWorkerTemplateEqual(t, &expectTemplates[i], &workerTemplate)
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
