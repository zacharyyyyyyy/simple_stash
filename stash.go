package main

import (
	"fmt"
	"os"
	"os/signal"
	"simple_stash/config"
	"simple_stash/handler"
	"simple_stash/input"
	"simple_stash/output"
	"syscall"
)

func main() {
	baseConf := &config.Client{}
	config.LoadConf(baseConf, "config.yaml")
	//初始化inputer ouputer
	inputHandler := input.NewInputer(input.KafkaInputer, *baseConf)
	outputHandler := output.NewOutputer(output.EsOutputer, *baseConf)
	//服务启动
	fmt.Println("stash running!")
	go handler.Start(inputHandler, outputHandler)
	//监听
	signs := make(chan os.Signal, 1)
	signal.Notify(signs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)
	select {
	case <-signs:
		fmt.Println("stash stopping!")
		handler.Stop()
	}
	fmt.Println("stash stop!")
}
