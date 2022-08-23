package main

import (
	"simple_stash/config"
	"simple_stash/handler"
	"simple_stash/input"
	"simple_stash/output"
)

func main() {
	baseConf := &config.Client{}
	config.LoadConf(baseConf, "config.yaml")
	//初始化inputer ouputer
	input.NewInputer(input.KafkaInputer, *baseConf)
	output.NewOutputer(output.EsOutputer, *baseConf)
	//服务启动
	handler.Start()
}
