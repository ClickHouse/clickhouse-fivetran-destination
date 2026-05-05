package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestAddUserReadableHintsToError(t *testing.T) {
	netErr := &net.OpError{Op: "read", Err: net.UnknownNetworkError("net error")}
	const op = "Failed to do thing"
	const unknownMsg = "Unexpected error in the ClickHouse destination. Please contact Fivetran support and include the technical details below."

	tests := []struct {
		name             string
		err              error
		expectedFriendly string
	}{
		{
			name:             "context_deadline",
			err:              fmt.Errorf("query: %w", context.DeadlineExceeded),
			expectedFriendly: "The ClickHouse operation took too long to complete. Retry the sync. If the problem persists, check the performance of the SQL executed. You may need to optimize batch sizes or scale up the ClickHouse service.",
		},
		{
			name:             "context_canceled",
			err:              fmt.Errorf("query: %w", context.Canceled),
			expectedFriendly: "The operation was cancelled before ClickHouse could complete it. Retry the sync.",
		},
		{
			name:             "ch_unknown_table",
			err:              &clickhouse.Exception{Code: 60, Message: "Unknown table"},
			expectedFriendly: "The target database or table does not exist in ClickHouse. Verify the schema and table names; the destination will create them on the next sync if needed.",
		},
		{
			name:             "ch_database_does_not_exist",
			err:              &clickhouse.Exception{Code: 81, Message: "Database does not exist"},
			expectedFriendly: "The target database or table does not exist in ClickHouse. Verify the schema and table names; the destination will create them on the next sync if needed.",
		},
		{
			name:             "ch_timeout_exceeded",
			err:              &clickhouse.Exception{Code: 159, Message: "Timeout exceeded"},
			expectedFriendly: "The ClickHouse query took too long to complete. Retry the sync. If the problem persists, check the performance of the SQL executed. You may need to optimize batch sizes or scale up the ClickHouse service.",
		},
		{
			name:             "ch_unknown_user",
			err:              &clickhouse.Exception{Code: 192, Message: "Unknown user"},
			expectedFriendly: "ClickHouse rejected the credentials. Verify the username and password configured for the destination.",
		},
		{
			name:             "ch_socket_timeout",
			err:              &clickhouse.Exception{Code: 209, Message: "Socket timeout"},
			expectedFriendly: "The ClickHouse query took too long to complete. Retry the sync. If the problem persists, check the performance of the SQL executed. You may need to optimize batch sizes or scale up the ClickHouse service.",
		},
		{
			name:             "ch_not_enough_privileges",
			err:              &clickhouse.Exception{Code: 497, Message: "Not enough privileges"},
			expectedFriendly: "The ClickHouse user is missing required privileges. Re-run the grants test and apply the privileges listed in the documentation.",
		},
		{
			name:             "ch_authentication_failed",
			err:              &clickhouse.Exception{Code: 516, Message: "Authentication failed"},
			expectedFriendly: "ClickHouse rejected the credentials. Verify the username and password configured for the destination.",
		},
		{
			name:             "ch_exception_wrapped",
			err:              fmt.Errorf("ExecStatement failed: %w", &clickhouse.Exception{Code: 497, Message: "Not enough privileges"}),
			expectedFriendly: "The ClickHouse user is missing required privileges. Re-run the grants test and apply the privileges listed in the documentation.",
		},
		{
			name:             "ch_unknown_code_falls_back_to_unknown",
			err:              &clickhouse.Exception{Code: 99999, Message: "Some new code we have not categorized"},
			expectedFriendly: unknownMsg,
		},
		{
			name:             "net_op_error",
			err:              netErr,
			expectedFriendly: "Could not reach the ClickHouse service. Verify the ClickHouse Cloud service is running and reachable from Fivetran (host, port, IP allowlist).",
		},
		{
			name:             "net_op_error_wrapped",
			err:              fmt.Errorf("ClickHouse connection error: ping failed after 10 attempts: %w", netErr),
			expectedFriendly: "Could not reach the ClickHouse service. Verify the ClickHouse Cloud service is running and reachable from Fivetran (host, port, IP allowlist).",
		},
		{
			name:             "io_eof",
			err:              io.EOF,
			expectedFriendly: "Could not reach the ClickHouse service. Verify the ClickHouse Cloud service is running and reachable from Fivetran (host, port, IP allowlist).",
		},
		{
			name:             "syscall_econnreset",
			err:              fmt.Errorf("read: %w", syscall.ECONNRESET),
			expectedFriendly: "Could not reach the ClickHouse service. Verify the ClickHouse Cloud service is running and reachable from Fivetran (host, port, IP allowlist).",
		},
		{
			name:             "plain_error",
			err:              errors.New("something blew up"),
			expectedFriendly: unknownMsg,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := fmt.Sprintf("%s: %s Technical details: %s", op, tc.expectedFriendly, tc.err)
			assert.Equal(t, want, addUserReadableHintsToError(op, tc.err))
		})
	}
}
