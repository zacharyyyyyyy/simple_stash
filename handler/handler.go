package handler

import (
	"context"
	"fmt"
	"simple_stash/input"
	"simple_stash/output"
	"sync"
)

func Start(ctx context.Context, inputHandler input.Input, ouputHandler output.Output) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("input running!")
		input.Run(ctx, inputHandler, output.Write)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("output running!")
		output.Run(ctx, ouputHandler)
	}()
	wg.Wait()
}
