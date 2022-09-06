package handler

import (
	"context"
	"fmt"
	"simple_stash/input"
	"simple_stash/output"
	"sync"
	"time"
)

type handle struct {
	inputCtx         context.Context
	inputCancelFunc  func()
	outputCtx        context.Context
	outputCancelFunc func()
	inputCloseChan   chan struct{}
	outputCloseChan  chan struct{}
	writeFunc        func(data interface{})
}

var handler *handle

func init() {
	inputCtx, inputCancelFunc := context.WithCancel(context.Background())
	outputCtx, outputCancelFunc := context.WithCancel(context.Background())
	inputCloseChan := make(chan struct{}, 1)
	outputCloseChan := make(chan struct{}, 1)
	handler = &handle{
		inputCtx:         inputCtx,
		inputCancelFunc:  inputCancelFunc,
		outputCtx:        outputCtx,
		outputCancelFunc: outputCancelFunc,
		inputCloseChan:   inputCloseChan,
		outputCloseChan:  outputCloseChan,
		writeFunc:        output.Write,
	}
}

func Start(inputHandler input.Input, ouputHandler output.Output) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("inputer running!")
		input.Run(handler.inputCtx, inputHandler, handler.writeFunc)
		handler.inputCloseChan <- struct{}{}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("outputer running!")
		output.Run(handler.outputCtx, ouputHandler)
		handler.outputCloseChan <- struct{}{}
	}()
	wg.Wait()
}

func Stop() {
	maxWaitTimer := time.NewTimer(10 * time.Second)
	handler.inputCancelFunc()
	select {
	case <-maxWaitTimer.C:
	case <-handler.inputCloseChan:
		handler.outputCancelFunc()
		select {
		case <-maxWaitTimer.C:
		case <-handler.outputCloseChan:
		}
	}
}
