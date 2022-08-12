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
		Customers   int           `yaml:"Customers"`
		MaxWaitTime int
	}
	EsConf struct {
		Username string `yaml:"Username"`
		Password string `yaml:"Password"`
		Host     string `yaml:"Host"`
		Port     string `yaml:"Port"`
		Index    string `yaml:"Index"`
	}
	ClientDetail struct {
		KafkaConf kafkaConf `yaml:"KafKa"`
		EsConf    EsConf    `yaml:"ElasticSearch"`
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
	fmt.Println()
}
