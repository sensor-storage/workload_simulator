package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

type InfluxDBConfig struct {
	Host         string
	Token        string
	Organization string
	Bucket       string
}

type WriterTemplate struct {
	Measurement  string
	Tags         []string `yaml:",flow"`
	Fields       []string `yaml:",flow"`
	Workers      int
	EmitInterval time.Duration `yaml:"emitInterval,flow"`
	BatchSize    uint          `yaml:"batchSize"`
}

type CheckerConfig struct {
	Query    string
	Interval time.Duration
	Name     string
}

type WorkloadConfig struct {
	Checkers        []CheckerConfig  `yaml:",flow"`
	WriterTemplates []WriterTemplate `yaml:"writerTemplates,flow"`
	LogPerWorks     uint             `yaml:"logPerWorks,flow"`
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
