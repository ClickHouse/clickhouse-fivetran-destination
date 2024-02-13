package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/avast/retry-go/v4"
)

// RetryNetError retries the given operation only if it returns a net.Error.
// Any other error will be returned immediately.
func RetryNetError(op retry.RetryableFunc, ctx context.Context, opName string) error {
	err := retry.Do(
		op,
		retry.Attempts(*maxRetries),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(time.Duration(*retryDelayMilliseconds)*time.Millisecond),
		retry.RetryIf(IsNetError),
		retry.Context(ctx))
	if err != nil {
		return fmt.Errorf("failed to execute %s: %w", opName, err)
	}
	return nil
}

// RetryNetErrorWithData retries the given operation only if it returns a net.Error.
// Any other error will be returned immediately.
func RetryNetErrorWithData[T any](op retry.RetryableFuncWithData[T], ctx context.Context, opName string) (T, error) {
	data, err := retry.DoWithData(
		op,
		retry.Attempts(*maxRetries),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(time.Duration(*retryDelayMilliseconds)*time.Millisecond),
		retry.RetryIf(IsNetError),
		retry.Context(ctx))
	if err != nil {
		var empty T
		return empty, fmt.Errorf("failed to execute %s: %w", opName, err)
	}
	return data, nil
}

func IsNetError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr)
}
