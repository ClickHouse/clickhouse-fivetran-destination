package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsNetError(t *testing.T) {
	assert.True(t, IsNetError(makeNetError()))
	assert.False(t, IsNetError(errors.New("not a net.Error")))
}

func TestGetBackoffDelay(t *testing.T) {
	assert.Equal(t, time.Duration(10), GetBackoffDelay(10, 100, 0))
	assert.Equal(t, time.Duration(10), GetBackoffDelay(10, 100, 1))
	assert.Equal(t, time.Duration(20), GetBackoffDelay(10, 100, 2))
	assert.Equal(t, time.Duration(40), GetBackoffDelay(10, 100, 3))
	assert.Equal(t, time.Duration(80), GetBackoffDelay(10, 100, 4))
	assert.Equal(t, time.Duration(100), GetBackoffDelay(10, 100, 5))
	assert.Equal(t, time.Duration(100), GetBackoffDelay(10, 100, 6))
	assert.Equal(t, time.Duration(100), GetBackoffDelay(10, 100, 64))
}

func TestGetRetryDelayConfig(t *testing.T) {
	td := teardown()
	defer td(t)

	*initialRetryDelayMilliseconds = 0
	*maxRetryDelayMilliseconds = 10
	initial, max := GetRetryDelayConfig()
	assert.Equal(t, time.Second, initial)
	assert.Equal(t, time.Millisecond*10, max)

	*initialRetryDelayMilliseconds = 50
	*maxRetryDelayMilliseconds = 0
	initial, max = GetRetryDelayConfig()
	assert.Equal(t, time.Millisecond*50, initial)
	assert.Equal(t, time.Millisecond*50, max)

	*initialRetryDelayMilliseconds = 42
	*maxRetryDelayMilliseconds = 144
	initial, max = GetRetryDelayConfig()
	assert.Equal(t, time.Millisecond*42, initial)
	assert.Equal(t, time.Millisecond*144, max)

	*initialRetryDelayMilliseconds = 144
	*maxRetryDelayMilliseconds = 42
	initial, max = GetRetryDelayConfig()
	assert.Equal(t, time.Millisecond*144, initial)
	assert.Equal(t, time.Millisecond*144, max)
}

func TestRetryNetError(t *testing.T) {
	defer setupSuite()(t)

	err := RetryNetError(func() error {
		return nil
	}, context.Background(), "TestRetryNetError")
	assert.NoError(t, err)

	count := 0
	err = RetryNetError(func() error {
		count++
		if count == 2 {
			return nil
		}
		return makeNetError()
	}, context.Background(), "TestRetryNetError")
	assert.NoError(t, err)

	count = 0
	err = RetryNetError(func() error {
		count++
		if count == 3 { // max retries == 2, will fail anyway
			return nil
		}
		return makeNetError()
	}, context.Background(), "TestRetryNetError")
	assert.ErrorContains(t, err, "failed to execute TestRetryNetError after 2 attempts")
}

func TestRetryNetErrorWithData(t *testing.T) {
	defer setupSuite()(t)

	type myType struct {
		value int
	}

	data, err := RetryNetErrorWithData(func() (int, error) {
		return 42, nil
	}, context.Background(), "TestRetryNetErrorWithData")
	assert.NoError(t, err)
	assert.Equal(t, 42, data)

	count := 0
	data, err = RetryNetErrorWithData(func() (int, error) {
		count++
		if count == 2 {
			return 144, nil
		}
		return 0, makeNetError()
	}, context.Background(), "TestRetryNetErrorWithData")
	assert.NoError(t, err)
	assert.Equal(t, 144, data)

	count = 0
	_, err = RetryNetErrorWithData(func() (int, error) {
		count++
		if count == 3 { // max retries == 2, will fail anyway
			return 1, nil
		}
		return 0, makeNetError()
	}, context.Background(), "TestRetryNetErrorWithData")
	assert.ErrorContains(t, err, "failed to execute TestRetryNetErrorWithData after 2 attempts")

	// also works with an arbitrary type
	dataT, err := RetryNetErrorWithData(func() (myType, error) {
		return myType{42}, nil
	}, context.Background(), "TestRetryNetErrorWithData(T)")
	assert.NoError(t, err)
	assert.Equal(t, myType{42}, dataT)

	count = 0
	dataT, err = RetryNetErrorWithData(func() (myType, error) {
		count++
		if count == 2 {
			return myType{144}, nil
		}
		return myType{}, makeNetError()
	}, context.Background(), "TestRetryNetErrorWithData(T)")
	assert.NoError(t, err)
	assert.Equal(t, myType{144}, dataT)

	count = 0
	_, err = RetryNetErrorWithData(func() (myType, error) {
		count++
		if count == 3 { // max retries == 2, will fail anyway
			return myType{42}, nil
		}
		return myType{}, makeNetError()
	}, context.Background(), "TestRetryNetErrorWithData(T)")
	assert.ErrorContains(t, err, "failed to execute TestRetryNetErrorWithData(T) after 2 attempts")
}

func TestRetryNetErrorDelayConfiguration(t *testing.T) {
	defer func() func(t *testing.T) {
		td := teardown()
		*maxRetries = 3
		*initialRetryDelayMilliseconds = 50
		return td
	}()(t)

	count := 0
	start := time.Now()
	err := RetryNetError(func() error {
		count++
		if count == 3 { // maxRetries == 3, will eventually succeed
			return nil
		}
		return makeNetError()
	}, context.Background(), "TestRetryNetError")
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start).Milliseconds(), int64(50*2))

	count = 0
	start = time.Now()
	data, err := RetryNetErrorWithData(func() (int, error) {
		count++
		if count == 3 { // maxRetries == 3, will eventually succeed
			return 144, nil
		}
		return 0, makeNetError()
	}, context.Background(), "TestRetryNetErrorWithData")
	assert.NoError(t, err)
	assert.Equal(t, 144, data)
	assert.GreaterOrEqual(t, time.Since(start).Milliseconds(), int64(50*2))
}

func setupSuite() func(t *testing.T) {
	td := teardown()
	*maxRetries = 2
	*initialRetryDelayMilliseconds = 10
	return td
}

func teardown() func(t *testing.T) {
	currentMaxRetries := *maxRetries
	currentMaxDelayMs := *initialRetryDelayMilliseconds
	return func(t *testing.T) {
		*maxRetries = currentMaxRetries
		*initialRetryDelayMilliseconds = currentMaxDelayMs
	}
}

func makeNetError() net.Error {
	return net.Error(&net.OpError{Op: "read", Err: net.UnknownNetworkError("net error")})
}
