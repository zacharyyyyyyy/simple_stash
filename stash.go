package main

import (
	"fmt"
	"simple_stash/config"
)

func main() {
	baseConf := &config.Client{}
	config.LoadConf(baseConf, "config.yaml")
	fmt.Println(baseConf.ClientConf.EsConf.Index)
}
