package output

import (
	"context"
	"simple_stash/config"
	"simple_stash/logger"
)

type (
	Output interface {
		new(config config.ClientOutput) Output
		run(ctx context.Context) error
	}
)

var (
	OutputerMap     = make(map[string]Output)
	OutputerHandler Output
	//通过channel 将采集端数据传递给输出端
	dataChan = make(chan interface{}, 200)
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

func Run(ctx context.Context, output Output) {
	errChan := make(chan struct{}, 1)
	go func() {
		err := output.run(ctx)
		if err != nil {
			logger.Runtime.Error(err.Error())
		}
		errChan <- struct{}{}
	}()
	select {
	case <-errChan:
	}
}

func read() <-chan interface{} {
	return dataChan
}

func register(name string, outputer Output) {
	if _, ok := OutputerMap[name]; !ok {
		OutputerMap[name] = outputer
	}
}
