package input

import (
	"context"
	"log"
	"simple_stash/config"
	"simple_stash/logger"
)

type (
	Input interface {
		new(config config.ClientInput) Input
		run(ctx context.Context, consumeFunc func(data interface{})) error
	}
)

var inputerMap = make(map[string]Input)

func NewInputer(InputerName string, config config.Client) Input {
	if inputer, ok := inputerMap[InputerName]; ok {
		return inputer.new(config.ClientConf.Input)
	} else {
		log.Fatal(InputerName + "not found!")
	}
	return nil
}

func Run(ctx context.Context, intputHandler Input, consumeFunc func(data interface{})) {
	errChan := make(chan struct{}, 1)
	go func() {
		err := intputHandler.run(ctx, consumeFunc)
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

func register(name string, inputer Input) {
	if _, ok := inputerMap[name]; !ok {
		inputerMap[name] = inputer
	}
}
