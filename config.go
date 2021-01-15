package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type InfluxDBConfig struct {
	Host         string
	Token        string
	Organization string
	Bucket       string
}

type WorkerTemplate struct {
	Measurement  string
	Tags         []string `yaml:",flow"`
	Fields       []string `yaml:",flow"`
	Workers      int
	EmitInterval int `yaml:"emitInterval,flow"`
}

type WorkloadConfig struct {
	WorkerTemplates []WorkerTemplate `yaml:"workerTemplates,flow"`
	LogPerWorks     int              `yaml:"logPerWorks,flow"`
}

type Config struct {
	InfluxDB InfluxDBConfig `yaml:"influxDB,flow"`
	Workload WorkloadConfig
}

func ParseConfig(data []byte) (*Config, error) {
	config := new(Config)
	err := yaml.Unmarshal(data, config)
	return config, err
}

func LoadConfigFile(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	config, err := ParseConfig(data)
	if err != nil {
		return nil, err
	}

	return config, nil
}
