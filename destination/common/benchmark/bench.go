package benchmark

import (
	"fmt"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/log"
)

func RunAndNotice(op func() error, opName string) error {
	start := time.Now()
	err := op()
	elapsed := time.Since(start) / time.Millisecond
	if err != nil {
		return fmt.Errorf("%s failed after %d ms: %w", opName, elapsed, err)
	}
	log.Notice(fmt.Sprintf("%s completed in %d ms", opName, elapsed))
	return nil
}

func RunAndNoticeWithData[T any](op func() (T, error), opName string) (T, error) {
	start := time.Now()
	data, err := op()
	elapsed := time.Since(start) / time.Millisecond
	if err != nil {
		return data, fmt.Errorf("%s failed after %d ms: %w", opName, elapsed, err)
	}
	log.Notice(fmt.Sprintf("%s completed in %d ms", opName, elapsed))
	return data, nil
}
