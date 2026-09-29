package db

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "read tcp 10.0.0.1:1->10.0.0.2:9440: i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestIsMutationPossiblyRunningErr(t *testing.T) {
	cases := map[string]struct {
		err  error
		want bool
	}{
		"replicas inactive (341)":        {&clickhouse.Exception{Code: 341}, true},
		"same query id running (216)":    {&clickhouse.Exception{Code: 216}, true},
		"wrapped 216":                    {fmt.Errorf("exec: %w", &clickhouse.Exception{Code: 216}), true},
		"other server error":             {&clickhouse.Exception{Code: 60}, false},
		"network read timeout":           {fmt.Errorf("query processing: %w", timeoutError{}), true},
		"timeout text without net.Error": {errors.New("failed to read packet: read: i/o timeout"), true},
		"context deadline":               {fmt.Errorf("exec: %w", context.DeadlineExceeded), true},
		"context cancelled":              {context.Canceled, true},
		"plain error":                    {errors.New("boom"), false},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, c.want, isMutationPossiblyRunningErr(c.err))
		})
	}
}
