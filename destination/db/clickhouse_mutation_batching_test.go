package db

import (
	"context"
	"sync/atomic"
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/constants"
	csvfile "fivetran.com/fivetran_sdk/destination/common/csv"
	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/common/types"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
)

const historyModeCSVFile = "../../tests/resources/history_mode.csv"

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

func openHistoryModeReader(t *testing.T) *csvfile.CSVFileReader {
	t.Helper()
	reader, err := csvfile.NewCSVFileReader(
		historyModeCSVFile,
		map[string][]byte{historyModeCSVFile: nil},
		pb.Compression_OFF,
		pb.Encryption_NONE,
	)
	assert.NoError(t, err)
	return reader
}

func TestUpdateForEarliestStartHistoryBatchesExec(t *testing.T) {
	original := *flags.MutationBatchSize
	defer func() { *flags.MutationBatchSize = original }()
	*flags.MutationBatchSize = 2

	mock := &mockConn{}
	conn := &ClickHouseConnection{Conn: mock, isLocal: true}

	reader := openHistoryModeReader(t)
	defer reader.Close()

	totalRows, err := conn.UpdateForEarliestStartHistory(
		context.Background(), "tester", historyModeTable(),
		reader, historyModeCSVColumns(), constants.FivetranStart,
	)
	assert.NoError(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Equal(t, int64(3), mock.execCount.Load(),
		"5 rows with MutationBatchSize=2 should produce 3 ExecStatement calls")
}

func TestUpdateForEarliestStartHistorySingleBatch(t *testing.T) {
	original := *flags.MutationBatchSize
	defer func() { *flags.MutationBatchSize = original }()
	*flags.MutationBatchSize = 1500

	mock := &mockConn{}
	conn := &ClickHouseConnection{Conn: mock, isLocal: true}

	reader := openHistoryModeReader(t)
	defer reader.Close()

	totalRows, err := conn.UpdateForEarliestStartHistory(
		context.Background(), "tester", historyModeTable(),
		reader, historyModeCSVColumns(), constants.FivetranStart,
	)
	assert.NoError(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Equal(t, int64(1), mock.execCount.Load(),
		"5 rows with MutationBatchSize=1500 should produce 1 ExecStatement call")
}
