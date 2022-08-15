package output

import (
	"context"
	"simple_stash/config"
)

type (
	OutputInit interface {
		new(config config.ClientOutput) Output
	}
	Output interface {
		write(ctx context.Context, data interface{})
	}
)

var OutputerMap = make(map[string]OutputInit)

func NewOutputer(OutputerName string, config config.Client) Output {
	if outputInit, ok := OutputerMap[OutputerName]; ok {
		return outputInit.new(config.ClientConf.Output)
	}
	return nil
}
func Write(ctx context.Context, outputHandler Output, data interface{}) {
	outputHandler.write(ctx, data)
}

func register(name string, outputer OutputInit) {
	if _, ok := OutputerMap[name]; !ok {
		OutputerMap[name] = outputer
	}
}
