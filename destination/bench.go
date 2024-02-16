package main

import (
	"fmt"
	"time"
)

func BenchmarkAndNotice(op func() error, opName string) error {
	start := time.Now()
	err := op()
	elapsed := time.Since(start) / time.Millisecond
	if err == nil {
		LogNotice(fmt.Sprintf("%s completed in %d ms", opName, elapsed))
	} else {
		LogNotice(fmt.Sprintf("%s failed after %d ms: %s", opName, elapsed, err))
	}
	return err
}

func BenchmarkAndNoticeWithData[T any](op func() (T, error), opName string) (T, error) {
	start := time.Now()
	data, err := op()
	elapsed := time.Since(start) / time.Millisecond
	if err == nil {
		LogNotice(fmt.Sprintf("%s completed in %d ms", opName, elapsed))
	} else {
		LogNotice(fmt.Sprintf("%s failed after %d ms: %s", opName, elapsed, err))
	}
	return data, err
}
