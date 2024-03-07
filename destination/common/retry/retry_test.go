package retry

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"fivetran.com/fivetran_sdk/destination/common/flags"
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

	*flags.InitialRetryDelayMilliseconds = 0
	*flags.MaxRetryDelayMilliseconds = 10
	initial, max := GetDelayConfig()
	assert.Equal(t, time.Second, initial)
	assert.Equal(t, time.Millisecond*10, max)

	*flags.InitialRetryDelayMilliseconds = 50
	*flags.MaxRetryDelayMilliseconds = 0
	initial, max = GetDelayConfig()
	assert.Equal(t, time.Millisecond*50, initial)
	assert.Equal(t, time.Millisecond*50, max)

	*flags.InitialRetryDelayMilliseconds = 42
	*flags.MaxRetryDelayMilliseconds = 144
	initial, max = GetDelayConfig()
	assert.Equal(t, time.Millisecond*42, initial)
	assert.Equal(t, time.Millisecond*144, max)

	*flags.InitialRetryDelayMilliseconds = 144
	*flags.MaxRetryDelayMilliseconds = 42
	initial, max = GetDelayConfig()
	assert.Equal(t, time.Millisecond*144, initial)
	assert.Equal(t, time.Millisecond*144, max)
}

func TestRetryNetError(t *testing.T) {
	defer setupSuite()(t)

	err := OnNetError(func() error {
		return nil
	}, context.Background(), "TestRetryNetError", false)
	assert.NoError(t, err)

	count := 0
	err = OnNetError(func() error {
		count++
		if count == 2 {
			return nil
		}
		return makeNetError()
	}, context.Background(), "TestRetryNetError", false)
	assert.NoError(t, err)

	count = 0
	err = OnNetError(func() error {
		count++
		if count == 3 { // max retries == 2, will fail anyway
			return nil
		}
		return makeNetError()
	}, context.Background(), "TestRetryNetError", false)
	assert.ErrorContains(t, err, "TestRetryNetError failed after 2 attempts")
}

func TestRetryNetErrorWithData(t *testing.T) {
	defer setupSuite()(t)

	type myType struct {
		value int
	}

	data, err := OnNetErrorWithData(func() (int, error) {
		return 42, nil
	}, context.Background(), "TestRetryNetErrorWithData", false)
	assert.NoError(t, err)
	assert.Equal(t, 42, data)

	count := 0
	data, err = OnNetErrorWithData(func() (int, error) {
		count++
		if count == 2 {
			return 144, nil
		}
		return 0, makeNetError()
	}, context.Background(), "TestRetryNetErrorWithData", false)
	assert.NoError(t, err)
	assert.Equal(t, 144, data)

	count = 0
	_, err = OnNetErrorWithData(func() (int, error) {
		count++
		if count == 3 { // max retries == 2, will fail anyway
			return 1, nil
		}
		return 0, makeNetError()
	}, context.Background(), "TestRetryNetErrorWithData", false)
	assert.ErrorContains(t, err, "TestRetryNetErrorWithData failed after 2 attempts")

	// also works with an arbitrary type
	dataT, err := OnNetErrorWithData(func() (myType, error) {
		return myType{42}, nil
	}, context.Background(), "TestRetryNetErrorWithData(T)", false)
	assert.NoError(t, err)
	assert.Equal(t, myType{42}, dataT)

	count = 0
	dataT, err = OnNetErrorWithData(func() (myType, error) {
		count++
		if count == 2 {
			return myType{144}, nil
		}
		return myType{}, makeNetError()
	}, context.Background(), "TestRetryNetErrorWithData(T)", false)
	assert.NoError(t, err)
	assert.Equal(t, myType{144}, dataT)

	count = 0
	_, err = OnNetErrorWithData(func() (myType, error) {
		count++
		if count == 3 { // max retries == 2, will fail anyway
			return myType{42}, nil
		}
		return myType{}, makeNetError()
	}, context.Background(), "TestRetryNetErrorWithData(T)", false)
	assert.ErrorContains(t, err, "TestRetryNetErrorWithData(T) failed after 2 attempts")
}

func TestRetryNetErrorDelayConfiguration(t *testing.T) {
	defer func() func(t *testing.T) {
		td := teardown()
		*flags.MaxRetries = 3
		*flags.InitialRetryDelayMilliseconds = 50
		return td
	}()(t)

	count := 0
	start := time.Now()
	err := OnNetError(func() error {
		count++
		if count == 3 { // maxRetries == 3, will eventually succeed
			return nil
		}
		return makeNetError()
	}, context.Background(), "TestRetryNetError", false)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start).Milliseconds(), int64(50*2))

	count = 0
	start = time.Now()
	data, err := OnNetErrorWithData(func() (int, error) {
		count++
		if count == 3 { // maxRetries == 3, will eventually succeed
			return 144, nil
		}
		return 0, makeNetError()
	}, context.Background(), "TestRetryNetErrorWithData", false)
	assert.NoError(t, err)
	assert.Equal(t, 144, data)
	assert.GreaterOrEqual(t, time.Since(start).Milliseconds(), int64(50*2))
}

func setupSuite() func(t *testing.T) {
	td := teardown()
	*flags.MaxRetries = 2
	*flags.InitialRetryDelayMilliseconds = 10
	return td
}

func teardown() func(t *testing.T) {
	currentMaxRetries := *flags.MaxRetries
	currentMaxDelayMs := *flags.InitialRetryDelayMilliseconds
	return func(t *testing.T) {
		*flags.MaxRetries = currentMaxRetries
		*flags.InitialRetryDelayMilliseconds = currentMaxDelayMs
	}
}

func makeNetError() net.Error {
	return net.Error(&net.OpError{Op: "read", Err: net.UnknownNetworkError("net error")})
}
