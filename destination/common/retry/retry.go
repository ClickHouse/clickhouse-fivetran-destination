package retry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/benchmark"
	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/common/log"
	"github.com/ClickHouse/clickhouse-go/v2"
)

// chCodeKeeperException is the ClickHouse server error code KEEPER_EXCEPTION.
// It surfaces transient ZooKeeper/Keeper failures such as "Session expired",
// connection loss, and operation timeouts — all worth retrying.
// Reference: https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
const chCodeKeeperException = 999

// OnNetError retries the given operation if it returns a transient error
// (network failure or a ClickHouse Keeper exception) using an exponential
// backoff strategy. Any other error will be returned immediately.
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
		if !IsRetryable(err) {
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
		return fmt.Errorf("%s failed after %d attempts: %w", opName, *flags.MaxRetries, err)
	}
	return nil
}

// OnNetErrorWithData retries the given operation if it returns a transient
// error (network failure or a ClickHouse Keeper exception) using an
// exponential backoff strategy. Any other error will be returned immediately.
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
		if !IsRetryable(err) {
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
		return empty, fmt.Errorf("%s failed after %d attempts: %w", opName, *flags.MaxRetries, err)
	}
	return data, nil
}

func OnFalseWithFixedDelay(
	op func() (bool, error),
	ctx context.Context,
	opName string,
	maxRetries uint,
	delay time.Duration,
) error {
	attempts := uint(1)
	for {
		isSuccess, err := op()
		if err != nil {
			return err
		}
		if isSuccess {
			return nil
		}
		attempts++
		if attempts > maxRetries {
			return fmt.Errorf("%s failed after %d attempts with %s interval between retries", opName, maxRetries, delay)
		}
		log.Notice(fmt.Sprintf("retrying %s (attempt %d)", opName, attempts))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			continue
		}
	}
}

// IsNetError returns true if err looks like a transient/recoverable network failure
// from the ClickHouse Go driver and should be retried.
//
// errors.As(net.Error) already covers most cases — including syscall.EPIPE /
// syscall.ECONNRESET (syscall.Errno has Timeout/Temporary methods) and net.ErrClosed
// (its underlying type does too). The explicit io.EOF branch is required because
// both ch-go and clickhouse-go wrap io.EOF in additional
func IsNetError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, io.EOF)
}

// IsKeeperException returns true if err is (or wraps) a ClickHouse server
// exception with code 999 (KEEPER_EXCEPTION). These are transient failures of
// the underlying ZooKeeper/Keeper layer — most commonly "Session expired",
// connection loss, or operation timeout — and should be retried.
func IsKeeperException(err error) bool {
	if err == nil {
		return false
	}
	var ex *clickhouse.Exception
	return errors.As(err, &ex) && ex.Code == chCodeKeeperException
}

// IsRetryable returns true if err represents a transient failure that the
// retry loops should back off on and retry
func IsRetryable(err error) bool {
	return IsNetError(err) || IsKeeperException(err)
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
