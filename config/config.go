package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"log"
	"os"
)

type (
	KafKaBroker struct {
		Host string `yaml:"Host"`
		Port string `yaml:"Port"`
		User string `yaml:"User"`
		Pwd  string `yaml:"Pwd"`
	}
	kafkaConf struct {
		Broker      []KafKaBroker `yaml:"Broker"`
		Topic       []string      `yaml:"Topic"`
		Group       string        `yaml:"Group"`
		Consumers   int           `yaml:"Consumers"`
		MaxWaitTime int           `yaml:"MaxWaitTime"`
	}
	EsConf struct {
		Username     string `yaml:"Username"`
		Password     string `yaml:"Password"`
		Host         string `yaml:"Host"`
		Port         string `yaml:"Port"`
		Index        string `yaml:"Index"`
		BulkMaxCount int    `yaml:"BulkMaxCount"`
	}
	ClientInput struct {
		KafkaConf kafkaConf `yaml:"KafKa"`
	}
	ClientOutput struct {
		EsConf EsConf `yaml:"ElasticSearch"`
	}
	ClientDetail struct {
		Input  ClientInput  `yaml:"Input"`
		Output ClientOutput `yaml:"Output"`
	}
	Client struct {
		ClientConf ClientDetail `yaml:"Client"`
	}
)

func LoadConf(config *Client, configFileName string) {
	var f *os.File
	f, err := os.Open(configFileName)
	if err != nil {
		log.Fatal(err)
	}
	err = yaml.NewDecoder(f).Decode(config)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(config)
}
