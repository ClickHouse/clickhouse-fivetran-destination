package db

import (
	"context"
	"sync/atomic"
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/constants"
	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/common/types"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
)

// mockConn embeds driver.Conn so it satisfies the interface with zero boilerplate.
// Only Exec is overridden; any other method called unexpectedly will panic (useful test signal).
type mockConn struct {
	driver.Conn
	execCount atomic.Int64
}

func (m *mockConn) Exec(ctx context.Context, query string, args ...any) error {
	m.execCount.Add(1)
	return nil
}

func historyModeCSVColumns() *types.CSVColumns {
	idCol := &types.CSVColumn{Index: 0, Name: "id", Type: pb.DataType_INT, IsPrimaryKey: true}
	startCol := &types.CSVColumn{Index: 1, Name: constants.FivetranStart, Type: pb.DataType_UTC_DATETIME, IsPrimaryKey: true}
	return &types.CSVColumns{
		All:         []*types.CSVColumn{idCol, startCol},
		PrimaryKeys: []*types.CSVColumn{idCol, startCol},
	}
}

func historyModeTable() *pb.Table {
	return &pb.Table{Name: "users"}
}

func TestUpdateForEarliestStartHistoryBatchesExec(t *testing.T) {
	original := *flags.MutationBatchSize
	defer func() { *flags.MutationBatchSize = original }()
	*flags.MutationBatchSize = 2

	mock := &mockConn{}
	conn := &ClickHouseConnection{Conn: mock, isLocal: true}

	csv := [][]string{
		{"1", "2025-11-11T20:57:00Z"},
		{"2", "2025-11-11T20:57:00Z"},
		{"3", "2025-11-11T20:57:00Z"},
		{"4", "2025-11-11T20:57:00Z"},
		{"5", "2025-11-11T20:57:00Z"},
	}

	err := conn.UpdateForEarliestStartHistory(
		context.Background(), "tester", historyModeTable(),
		csv, historyModeCSVColumns(), constants.FivetranStart,
	)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), mock.execCount.Load(),
		"5 rows with MutationBatchSize=2 should produce 3 ExecStatement calls")
}

func TestUpdateForEarliestStartHistorySingleBatch(t *testing.T) {
	original := *flags.MutationBatchSize
	defer func() { *flags.MutationBatchSize = original }()
	*flags.MutationBatchSize = 1500

	mock := &mockConn{}
	conn := &ClickHouseConnection{Conn: mock, isLocal: true}

	csv := [][]string{
		{"1", "2025-11-11T20:57:00Z"},
		{"2", "2025-11-11T20:57:00Z"},
	}

	err := conn.UpdateForEarliestStartHistory(
		context.Background(), "tester", historyModeTable(),
		csv, historyModeCSVColumns(), constants.FivetranStart,
	)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), mock.execCount.Load(),
		"2 rows with MutationBatchSize=1500 should produce 1 ExecStatement call")
}
