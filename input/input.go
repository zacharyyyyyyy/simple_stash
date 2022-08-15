package input

import (
	"context"
	"simple_stash/config"
	"simple_stash/logger"
)

type (
	InputInit interface {
		new(config config.ClientInput) Input
	}
	Input interface {
		collect(ctx context.Context) error
	}
)

var InputerMap = make(map[string]InputInit)

func NewInputer(InputerName string, config config.Client) Input {
	if inputer, ok := InputerMap[InputerName]; ok {
		return inputer.new(config.ClientConf.Input)
	}
	return nil
}

func Collect(ctx context.Context, intputHandler Input) {
	errChan := make(chan struct{}, 1)
	go func() {
		err := intputHandler.collect(ctx)
		if err != nil {
			logger.Runtime.Error(err.Error())
		}
		errChan <- struct{}{}
	}()
	select {
	case <-ctx.Done():
	case <-errChan:
	}

}

func register(name string, inputer InputInit) {
	if _, ok := InputerMap[name]; !ok {
		InputerMap[name] = inputer
	}
}
