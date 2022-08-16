package output

import (
	"simple_stash/config"
	"simple_stash/logger"
)

type (
	OutputInit interface {
		new(config config.ClientOutput) Output
	}
	Output interface {
		run() error
	}
)

var (
	OutputerMap     = make(map[string]OutputInit)
	OutputerHandler Output
	dataChan        = make(chan interface{}, 200)
)

func NewOutputer(OutputerName string, config config.Client) Output {
	if outputInit, ok := OutputerMap[OutputerName]; ok {
		OutputerHandler = outputInit.new(config.ClientConf.Output)
		return OutputerHandler
	}
	return nil
}
func Write(data interface{}) {
	dataChan <- data
}

func Run(output Output) {
	errChan := make(chan struct{}, 1)
	go func() {
		err := output.run()
		if err != nil {
			logger.Runtime.Error(err.Error())
		}
		errChan <- struct{}{}
	}()
	select {
	case <-errChan:
	}
}

func register(name string, outputer OutputInit) {
	if _, ok := OutputerMap[name]; !ok {
		OutputerMap[name] = outputer
	}
}
