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
	if err == nil {
		log.Notice(fmt.Sprintf("%s completed in %d ms", opName, elapsed))
	} else {
		log.Notice(fmt.Sprintf("%s failed after %d ms: %s", opName, elapsed, err))
	}
	return err
}

func RunAndNoticeWithData[T any](op func() (T, error), opName string) (T, error) {
	start := time.Now()
	data, err := op()
	elapsed := time.Since(start) / time.Millisecond
	if err == nil {
		log.Notice(fmt.Sprintf("%s completed in %d ms", opName, elapsed))
	} else {
		log.Notice(fmt.Sprintf("%s failed after %d ms: %s", opName, elapsed, err))
	}
	return data, err
}
