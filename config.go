package main

import (
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v2"
)

//InfluxDBConfig is ...
type InfluxDBConfig struct {
	Host         string
	Token        string
	Organization string
	Bucket       string
}

//WriterTemplate is ...
type WriterTemplate struct {
	Measurement  string
	Tags         []string `yaml:",flow"`
	Fields       []string `yaml:",flow"`
	Workers      int
	EmitInterval time.Duration `yaml:"emitInterval,flow"`
	BatchSize    uint          `yaml:"batchSize"`
}

//WriterWithoutBatchTemplate is ...
type WriterWithoutBatchTemplate struct {
	Measurement  string
	Tags         []string `yaml:",flow"`
	Fields       []string `yaml:",flow"`
	Workers      int
	EmitInterval time.Duration `yaml:"emitInterval,flow"`
}

//CheckerConfig is ...
type CheckerConfig struct {
	Query         string
	Interval      time.Duration
	Name          string
	CheckerNumber int
}

//WorkloadConfig is ...
type WorkloadConfig struct {
	Checkers                    []CheckerConfig  `yaml:",flow"`
	WriterTemplates             []WriterTemplate `yaml:"writerTemplates,flow"`
	WriterWithoutBatchTemplates []WriterTemplate `yaml:"writerWithoutBatchTemplates,flow"`
	LogPerWorks                 uint             `yaml:"logPerWorks,flow"`
}

//Config is ...
type Config struct {
	InfluxDB InfluxDBConfig `yaml:"influxDB,flow"`
	Workload WorkloadConfig
}

//ParseConfig is ...
func ParseConfig(data []byte) (*Config, error) {
	config := new(Config)
	err := yaml.Unmarshal(data, config)
	return config, err
}

//LoadConfigFile is ...
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
