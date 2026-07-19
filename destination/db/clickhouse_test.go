package db

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapErr(t *testing.T) {
	errBoom := errors.New("boom")
	double := func(v int) (int, error) {
		if v < 0 {
			return 0, fmt.Errorf("negative value %d: %w", v, errBoom)
		}
		return v * 2, nil
	}

	collect := func(input []int) (values []int, errs []error) {
		for v, err := range mapErr(slices.Values(input), double) {
			values = append(values, v)
			errs = append(errs, err)
		}
		return values, errs
	}

	t.Run("transforms all values in order", func(t *testing.T) {
		values, errs := collect([]int{1, 2, 3})
		assert.Equal(t, []int{2, 4, 6}, values)
		for _, err := range errs {
			assert.NoError(t, err)
		}
	})

	t.Run("empty sequence yields nothing", func(t *testing.T) {
		values, errs := collect(nil)
		assert.Empty(t, values)
		assert.Empty(t, errs)
	})

	t.Run("yields transform errors alongside values", func(t *testing.T) {
		values, errs := collect([]int{1, -1, 3})
		assert.Equal(t, []int{2, 0, 6}, values)
		require.Len(t, errs, 3)
		assert.NoError(t, errs[0])
		assert.ErrorIs(t, errs[1], errBoom)
		assert.NoError(t, errs[2])
	})

	t.Run("stops transforming when the consumer breaks", func(t *testing.T) {
		calls := 0
		counting := func(v int) (int, error) {
			calls++
			return v, nil
		}
		for range mapErr(slices.Values([]int{1, 2, 3}), counting) {
			break
		}
		assert.Equal(t, 1, calls, "transform must not run after the consumer stops iterating")
	})

	t.Run("is re-iterable with identical results", func(t *testing.T) {
		seq := mapErr(slices.Values([]int{1, 2, 3}), double)
		first := slices.Collect(func(yield func(int) bool) {
			for v := range seq {
				if !yield(v) {
					return
				}
			}
		})
		second := slices.Collect(func(yield func(int) bool) {
			for v := range seq {
				if !yield(v) {
					return
				}
			}
		})
		assert.Equal(t, []int{2, 4, 6}, first)
		assert.Equal(t, first, second, "sequence must be safe to iterate more than once (retry contract)")
	})
}
