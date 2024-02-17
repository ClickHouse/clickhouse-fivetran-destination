package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// RetryNetError retries the given operation only if it returns a net.Error using exponential backoff strategy.
// Any other error will be returned immediately.
// Execution time of all operations is measured and logged as notice.
func RetryNetError(
	op func() error,
	ctx context.Context,
	opName string,
	benchmark bool,
) (err error) {
	initialDelay, maxDelay := GetRetryDelayConfig()
	failCount := uint(0)
	for {
		if benchmark {
			err = BenchmarkAndNotice(op, opName)
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
		LogWarn(fmt.Sprintf("retrying %s, cause: %s", opName, err))
		if failCount < *maxRetries {
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
		return fmt.Errorf("failed to execute %s after %d attempts: %w", opName, *maxRetries, err)
	}
	return nil
}

// RetryNetErrorWithData retries the given operation only if it returns a net.Error using exponential backoff strategy.
// Any other error will be returned immediately.
// Execution time of all operations is measured and logged as notice.
func RetryNetErrorWithData[T any](
	op func() (T, error),
	ctx context.Context,
	opName string,
	benchmark bool,
) (data T, err error) {
	initialDelay, maxDelay := GetRetryDelayConfig()
	failCount := uint(0)
	for {
		if benchmark {
			data, err = BenchmarkAndNoticeWithData(op, opName)
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
		LogWarn(fmt.Sprintf("retrying %s, cause: %s", opName, err))
		if failCount < *maxRetries {
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
		return empty, fmt.Errorf("failed to execute %s after %d attempts: %w", opName, *maxRetries, err)
	}
	return data, nil
}

func IsNetError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr)
}

func GetRetryDelayConfig() (initial time.Duration, max time.Duration) {
	if *initialRetryDelayMilliseconds == 0 {
		initial = time.Second
	} else {
		initial = time.Duration(*initialRetryDelayMilliseconds) * time.Millisecond
	}
	if *maxRetryDelayMilliseconds == 0 || *maxRetryDelayMilliseconds < *initialRetryDelayMilliseconds {
		return initial, initial
	}
	return initial, time.Duration(*maxRetryDelayMilliseconds) * time.Millisecond
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
