package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
	assert.ErrorContains(t, err, "failed to execute TestRetryNetError: All attempts fail")
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
	assert.ErrorContains(t, err, "failed to execute TestRetryNetErrorWithData: All attempts fail")

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
	assert.ErrorContains(t, err, "failed to execute TestRetryNetErrorWithData(T): All attempts fail")
}

func TestRetryNetErrorDelayConfiguration(t *testing.T) {
	defer func() func(t *testing.T) {
		td := teardown()
		*maxRetries = 3
		*retryDelayMilliseconds = 50
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
	*retryDelayMilliseconds = 10
	return td
}

func teardown() func(t *testing.T) {
	currentMaxRetries := *maxRetries
	currentMaxDelayMs := *retryDelayMilliseconds
	return func(t *testing.T) {
		*maxRetries = currentMaxRetries
		*retryDelayMilliseconds = currentMaxDelayMs
	}
}

func makeNetError() net.Error {
	return net.Error(&net.OpError{Op: "read", Err: net.UnknownNetworkError("net error")})
}
