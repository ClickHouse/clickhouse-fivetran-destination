package retry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/benchmark"
	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/common/log"
)

// OnNetError retries the given operation only if it returns a net.Error using exponential backoff strategy.
// Any other error will be returned immediately.
// Execution time of all operations is measured and logged as notice.
func OnNetError(
	op func() error,
	ctx context.Context,
	opName string,
	withBenchmark bool,
) (err error) {
	initialDelay, maxDelay := GetDelayConfig()
	failCount := uint(0)
	for {
		if withBenchmark {
			err = benchmark.RunAndNotice(op, opName)
		} else {
			err = op()
		}
		if err == nil {
			return nil
		}
		if !IsNetError(err) {
			return err
		}
		failCount++
		log.Warn(fmt.Sprintf("retrying %s, cause: %s", opName, err))
		if failCount < *flags.MaxRetries {
			delay := GetBackoffDelay(initialDelay, maxDelay, failCount)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				continue
			}
		} else {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("failed to execute %s after %d attempts: %w", opName, *flags.MaxRetries, err)
	}
	return nil
}

// OnNetErrorWithData retries the given operation only if it returns a net.Error using exponential backoff strategy.
// Any other error will be returned immediately.
// Execution time of all operations is measured and logged as notice.
func OnNetErrorWithData[T any](
	op func() (T, error),
	ctx context.Context,
	opName string,
	withBenchmark bool,
) (data T, err error) {
	initialDelay, maxDelay := GetDelayConfig()
	failCount := uint(0)
	for {
		if withBenchmark {
			data, err = benchmark.RunAndNoticeWithData(op, opName)
		} else {
			data, err = op()
		}
		if err == nil {
			return data, nil
		}
		if !IsNetError(err) {
			var empty T
			return empty, err
		}
		failCount++
		log.Warn(fmt.Sprintf("retrying %s, cause: %s", opName, err))
		if failCount < *flags.MaxRetries {
			delay := GetBackoffDelay(initialDelay, maxDelay, failCount)
			select {
			case <-ctx.Done():
				var empty T
				return empty, ctx.Err()
			case <-time.After(delay):
				continue
			}
		} else {
			break
		}
	}
	if err != nil {
		var empty T
		return empty, fmt.Errorf("failed to execute %s after %d attempts: %w", opName, *flags.MaxRetries, err)
	}
	return data, nil
}

func IsNetError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr)
}

func GetDelayConfig() (initial time.Duration, max time.Duration) {
	if *flags.InitialRetryDelayMilliseconds == 0 {
		initial = time.Second
	} else {
		initial = time.Duration(*flags.InitialRetryDelayMilliseconds) * time.Millisecond
	}
	if *flags.MaxRetryDelayMilliseconds == 0 || *flags.MaxRetryDelayMilliseconds < *flags.InitialRetryDelayMilliseconds {
		return initial, initial
	}
	return initial, time.Duration(*flags.MaxRetryDelayMilliseconds) * time.Millisecond
}

func GetBackoffDelay(initialDelay time.Duration, maxDelay time.Duration, failCount uint) time.Duration {
	if failCount == 0 {
		return initialDelay
	}
	if failCount > 63 {
		return maxDelay
	}
	delay := initialDelay << (failCount - 1)
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}
