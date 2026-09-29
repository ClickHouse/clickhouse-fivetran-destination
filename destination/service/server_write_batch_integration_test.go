package service

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/db"
	"fivetran.com/fivetran_sdk/destination/db/config"
	pb "fivetran.com/fivetran_sdk/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Behavioural tests for the delete_files phase of WriteBatch, Fivetran's hard deletes in soft-delete mode: they
// only drive the gRPC handlers and read the table back.

type standardRow struct {
	contactID int64
	name      string
	value     string
}

type standardHarness struct {
	t         *testing.T
	ctx       context.Context
	conn      *db.ClickHouseConnection
	server    *Server
	table     *pb.Table
	tableName string
}

func newStandardHarness(t *testing.T) *standardHarness {
	t.Helper()
	ctx := context.Background()
	connConfig, err := config.Parse(integrationConfiguration)
	require.NoError(t, err)
	conn, err := db.GetClickHouseConnection(ctx, connConfig)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck
	require.NoError(t, conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+integrationSchema))

	h := &standardHarness{
		t: t, ctx: ctx, conn: conn, server: &Server{},
		tableName: "live_" + strings.ReplaceAll(uuid.New().String(), "-", "_"),
	}
	h.table = &pb.Table{Name: h.tableName, Columns: []*pb.Column{
		{Name: "contact_id", Type: pb.DataType_LONG, PrimaryKey: true},
		{Name: "name", Type: pb.DataType_STRING, PrimaryKey: true},
		{Name: "value", Type: pb.DataType_STRING},
		{Name: "_fivetran_synced", Type: pb.DataType_UTC_DATETIME},
		{Name: "_fivetran_deleted", Type: pb.DataType_BOOLEAN},
	}}
	resp, err := h.server.CreateTable(ctx, &pb.CreateTableRequest{
		Configuration: integrationConfiguration, SchemaName: integrationSchema, Table: h.table,
	})
	require.NoError(t, err)
	require.Nil(t, resp.GetTask(), "CreateTable failed: %s", resp.GetTask().GetMessage())
	t.Cleanup(func() {
		assert.NoError(t, conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s SYNC", integrationSchema, h.tableName)))
	})
	return h
}

func (h *standardHarness) seed(rows ...standardRow) {
	h.t.Helper()
	values := make([]string, 0, len(rows))
	for _, r := range rows {
		values = append(values, fmt.Sprintf("(%d, '%s', '%s', fromUnixTimestamp64Milli(1704067200000, 'UTC'), false)", r.contactID, r.name, r.value))
	}
	require.NoError(h.t, h.conn.Exec(h.ctx, fmt.Sprintf("INSERT INTO %s.%s VALUES %s", integrationSchema, h.tableName, strings.Join(values, ","))))
}

// rows are "contact_id,name" lines; delete files carry every table column
func (h *standardHarness) deleteFile(rows ...string) string {
	h.t.Helper()
	lines := make([]string, 0, len(rows))
	for _, r := range rows {
		lines = append(lines, r+",v,2024-01-01T00:00:00Z,true")
	}
	return (&historyHarness{t: h.t}).csvFile("contact_id,name,value,_fivetran_synced,_fivetran_deleted", lines)
}

func (h *standardHarness) writeDelete(files ...string) {
	h.t.Helper()
	resp := h.writeDeleteResponse(files...)
	require.True(h.t, resp.GetSuccess(), "WriteBatch failed: %s", resp.GetTask().GetMessage())
}

func (h *standardHarness) writeDeleteResponse(files ...string) *pb.WriteBatchResponse {
	h.t.Helper()
	keys := make(map[string][]byte, len(files))
	for _, f := range files {
		keys[f] = nil
	}
	resp, err := h.server.WriteBatch(h.ctx, &pb.WriteBatchRequest{
		Configuration: integrationConfiguration,
		SchemaName:    integrationSchema,
		Table:         h.table,
		Keys:          keys,
		DeleteFiles:   files,
		FileParams: &pb.FileParams{
			Compression: pb.Compression_OFF, Encryption: pb.Encryption_NONE,
			NullString: "null-m8yboxSY", UnmodifiedString: "unmod-NcK9NIuqUf",
		},
	})
	require.NoError(h.t, err)
	return resp
}

func (h *standardHarness) rows() []standardRow {
	h.t.Helper()
	rows, err := h.conn.Query(h.ctx, fmt.Sprintf("SELECT `contact_id`, `name`, ifNull(`value`, '') FROM %s.%s FINAL ORDER BY `contact_id`, `name`", integrationSchema, h.tableName))
	require.NoError(h.t, err)
	defer rows.Close() //nolint:errcheck
	var result []standardRow
	for rows.Next() {
		var r standardRow
		require.NoError(h.t, rows.Scan(&r.contactID, &r.name, &r.value))
		result = append(result, r)
	}
	return result
}

func (h *standardHarness) helperTables() uint64 {
	h.t.Helper()
	var count uint64
	require.NoError(h.t, h.conn.QueryRow(h.ctx, fmt.Sprintf(
		"SELECT count() FROM system.tables WHERE database = '%s' AND name LIKE '%s_%%'", integrationSchema, h.tableName)).Scan(&count))
	return count
}

func TestWriteBatchDeleteFiles(t *testing.T) {
	t.Run("rows with a staged key are removed, others untouched", func(t *testing.T) {
		// (1,a) removed; (1,b) shares contact_id only and stays; 2 is not in the file; 3 is unknown
		h := newStandardHarness(t)
		h.seed(standardRow{1, "a", "v"}, standardRow{1, "b", "v"}, standardRow{2, "a", "v"})
		h.writeDelete(h.deleteFile("1,a", "3,a"))
		assert.Equal(t, []standardRow{{1, "b", "v"}, {2, "a", "v"}}, h.rows())
		assert.Equal(t, uint64(0), h.helperTables())
	})

	t.Run("header only file changes nothing", func(t *testing.T) {
		h := newStandardHarness(t)
		h.seed(standardRow{10, "a", "v"})
		h.writeDelete(h.deleteFile())
		assert.Equal(t, []standardRow{{10, "a", "v"}}, h.rows())
		assert.Equal(t, uint64(0), h.helperTables())
	})

	t.Run("replaying the same file changes nothing", func(t *testing.T) {
		h := newStandardHarness(t)
		h.seed(standardRow{20, "a", "v"}, standardRow{21, "a", "v"})
		file := h.deleteFile("20,a")
		h.writeDelete(file)
		h.writeDelete(file)
		assert.Equal(t, []standardRow{{21, "a", "v"}}, h.rows())
	})

	t.Run("invalid value fails the batch and leaves no helper table", func(t *testing.T) {
		h := newStandardHarness(t)
		h.seed(standardRow{30, "a", "v"})
		resp := h.writeDeleteResponse(h.deleteFile("not-a-number,a"))
		require.NotNil(t, resp.GetTask(), "expected a failed response")
		assert.Contains(t, resp.GetTask().GetMessage(), "not-a-number")
		assert.Equal(t, []standardRow{{30, "a", "v"}}, h.rows())
		assert.Equal(t, uint64(0), h.helperTables())
	})

	t.Run("files larger than the batch sizes are fully applied", func(t *testing.T) {
		restore := []struct {
			flag *uint
			old  uint
			new  uint
		}{
			{flags.WriteBatchSize, *flags.WriteBatchSize, 3},
			{flags.StagingBatchSize, *flags.StagingBatchSize, 2},
		}
		for _, r := range restore {
			*r.flag = r.new
		}
		defer func() {
			for _, r := range restore {
				*r.flag = r.old
			}
		}()
		h := newStandardHarness(t)
		var seeded []standardRow
		var lines []string
		for id := int64(40); id < 45; id++ {
			seeded = append(seeded, standardRow{id, "a", "v"})
			lines = append(lines, fmt.Sprintf("%d,a", id))
		}
		h.seed(append(seeded, standardRow{45, "a", "v"})...)
		h.writeDelete(h.deleteFile(lines...))
		assert.Equal(t, []standardRow{{45, "a", "v"}}, h.rows())
		assert.Equal(t, uint64(0), h.helperTables())
	})
}
