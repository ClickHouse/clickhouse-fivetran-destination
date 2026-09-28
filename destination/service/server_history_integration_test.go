package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

// Behavioural tests for the earliest_start_files phase of WriteHistoryBatch. They only drive the
// gRPC handlers and read the table back, so they describe the contract regardless of how the
// phase is implemented. Times follow Fivetran's history mode guide: T100 -> 01:00, T105 -> 01:05, etc.

const (
	historySchema = "fivetran_test"
	historyMaxEnd = "2262-04-11 23:47:16.000000000"
)

var historyTestConfiguration = map[string]string{
	"host": "localhost", "port": "9000", "username": "default", "local": "true",
}

type historyVersion struct {
	contactID int64
	name      string
	value     string
	start     string
	end       string
	active    bool
	syncedMs  int64
}

func chTime(hhmm string) string { return "2024-01-01 " + hhmm + ":00.000000000" }

func csvTime(hhmm string) string { return "2024-01-01T" + hhmm + ":00Z" }

// _fivetran_synced is 2024-01-01 00:00:00 (1704067200000 ms) for every seeded version
func activeVersion(contactID int64, name, value, start string) historyVersion {
	return historyVersion{contactID, name, value, chTime(start), historyMaxEnd, true, 1704067200000}
}

func closedVersion(contactID int64, name, value, start, end string) historyVersion {
	return historyVersion{contactID, name, value, chTime(start), end, false, 1704067200000}
}

type historyHarness struct {
	t         *testing.T
	ctx       context.Context
	conn      *db.ClickHouseConnection
	server    *Server
	table     *pb.Table
	tableName string
}

func newHistoryHarness(t *testing.T) *historyHarness {
	t.Helper()
	ctx := context.Background()
	connConfig, err := config.Parse(historyTestConfiguration)
	require.NoError(t, err)
	conn, err := db.GetClickHouseConnection(ctx, connConfig)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck
	require.NoError(t, conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+historySchema))

	h := &historyHarness{
		t: t, ctx: ctx, conn: conn, server: &Server{},
		tableName: "history_" + strings.ReplaceAll(uuid.New().String(), "-", "_"),
	}
	h.table = &pb.Table{Name: h.tableName, Columns: []*pb.Column{
		{Name: "contact_id", Type: pb.DataType_LONG, PrimaryKey: true},
		{Name: "name", Type: pb.DataType_STRING, PrimaryKey: true},
		{Name: "value", Type: pb.DataType_STRING},
		{Name: "_fivetran_synced", Type: pb.DataType_UTC_DATETIME},
		{Name: "_fivetran_start", Type: pb.DataType_UTC_DATETIME, PrimaryKey: true},
		{Name: "_fivetran_end", Type: pb.DataType_UTC_DATETIME},
		{Name: "_fivetran_active", Type: pb.DataType_BOOLEAN},
	}}
	resp, err := h.server.CreateTable(ctx, &pb.CreateTableRequest{
		Configuration: historyTestConfiguration, SchemaName: historySchema, Table: h.table,
	})
	require.NoError(t, err)
	require.Nil(t, resp.GetTask(), "CreateTable failed: %s", resp.GetTask().GetMessage())
	t.Cleanup(func() {
		assert.NoError(t, conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s SYNC", historySchema, h.tableName)))
	})
	return h
}

func (h *historyHarness) seed(versions ...historyVersion) {
	h.t.Helper()
	values := make([]string, 0, len(versions))
	for _, v := range versions {
		active := "true"
		if !v.active {
			active = "false"
		}
		values = append(values, fmt.Sprintf("(%d, '%s', '%s', fromUnixTimestamp64Milli(%d, 'UTC'), '%s', '%s', %s)",
			v.contactID, v.name, v.value, v.syncedMs, v.start, v.end, active))
	}
	require.NoError(h.t, h.conn.Exec(h.ctx, fmt.Sprintf("INSERT INTO %s.%s VALUES %s", historySchema, h.tableName, strings.Join(values, ","))))
}

func (h *historyHarness) seedNullActive(contactID int64, name, value, start string) {
	h.t.Helper()
	require.NoError(h.t, h.conn.Exec(h.ctx, fmt.Sprintf(
		"INSERT INTO %s.%s VALUES (%d, '%s', '%s', fromUnixTimestamp64Milli(1704067200000, 'UTC'), '%s', '%s', NULL)",
		historySchema, h.tableName, contactID, name, value, chTime(start), historyMaxEnd)))
}

// rows are "contact_id,name,earliest start" lines
func (h *historyHarness) earliestStartFile(rows ...string) string {
	h.t.Helper()
	path := filepath.Join(h.t.TempDir(), fmt.Sprintf("earliest_%s.csv", uuid.New().String()))
	content := "contact_id,name,_fivetran_start\n"
	if len(rows) > 0 {
		content += strings.Join(rows, "\n") + "\n"
	}
	require.NoError(h.t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func (h *historyHarness) writeEarliestStart(files ...string) {
	h.t.Helper()
	resp := h.writeEarliestStartResponse(files...)
	require.True(h.t, resp.GetSuccess(), "WriteHistoryBatch failed: %s", resp.GetTask().GetMessage())
}

func (h *historyHarness) writeEarliestStartResponse(files ...string) *pb.WriteBatchResponse {
	h.t.Helper()
	keys := make(map[string][]byte, len(files))
	for _, f := range files {
		keys[f] = nil
	}
	resp, err := h.server.WriteHistoryBatch(h.ctx, &pb.WriteHistoryBatchRequest{
		Configuration:      historyTestConfiguration,
		SchemaName:         historySchema,
		Table:              h.table,
		Keys:               keys,
		EarliestStartFiles: files,
		FileParams: &pb.FileParams{
			Compression: pb.Compression_OFF, Encryption: pb.Encryption_NONE,
			NullString: "null-m8yboxSY", UnmodifiedString: "unmod-NcK9NIuqUf",
		},
	})
	require.NoError(h.t, err)
	return resp
}

func (h *historyHarness) rows() []historyVersion {
	h.t.Helper()
	rows, err := h.conn.Query(h.ctx, fmt.Sprintf("SELECT `contact_id`, `name`, ifNull(`value`, ''), toString(`_fivetran_start`), "+
		"ifNull(toString(`_fivetran_end`), ''), ifNull(`_fivetran_active`, false), toUnixTimestamp64Milli(`_fivetran_synced`) "+
		"FROM %s.%s FINAL ORDER BY `contact_id`, `name`, `_fivetran_start`", historySchema, h.tableName))
	require.NoError(h.t, err)
	defer rows.Close() //nolint:errcheck
	var result []historyVersion
	for rows.Next() {
		var v historyVersion
		require.NoError(h.t, rows.Scan(&v.contactID, &v.name, &v.value, &v.start, &v.end, &v.active, &v.syncedMs))
		result = append(result, v)
	}
	return result
}

func (h *historyHarness) helperTables() uint64 {
	h.t.Helper()
	var count uint64
	require.NoError(h.t, h.conn.QueryRow(h.ctx, fmt.Sprintf(
		"SELECT count() FROM system.tables WHERE database = '%s' AND name LIKE '%s_%%'", historySchema, h.tableName)).Scan(&count))
	return count
}

func TestWriteHistoryBatchEarliestStart(t *testing.T) {
	t.Run("fivetran guide example", func(t *testing.T) {
		// id 1 -> T150: the active T200 version overlaps and is removed, the closed T100 version is untouched.
		// ids 2, 3 -> T105: nothing overlaps, the active versions are closed at T105. id 4 is not in the file.
		h := newHistoryHarness(t)
		h.seed(
			closedVersion(1, "a", "abc", "01:00", "2024-01-01 01:59:59.999000000"),
			activeVersion(1, "a", "pqr", "02:00"),
			activeVersion(2, "a", "mno", "01:02"),
			activeVersion(3, "a", "xyz", "01:03"),
			activeVersion(4, "a", "lmn", "01:04"),
		)
		h.writeEarliestStart(h.earliestStartFile("1,a,"+csvTime("01:50"), "2,a,"+csvTime("01:05"), "3,a,"+csvTime("01:05")))
		assert.Equal(t, []historyVersion{
			closedVersion(1, "a", "abc", "01:00", "2024-01-01 01:59:59.999000000"),
			closedVersion(2, "a", "mno", "01:02", chTime("01:05")),
			closedVersion(3, "a", "xyz", "01:03", chTime("01:05")),
			activeVersion(4, "a", "lmn", "01:04"),
		}, h.rows())
		assert.Equal(t, uint64(0), h.helperTables())
	})

	t.Run("composite key isolates rows sharing one key column", func(t *testing.T) {
		// Same contact_id, different property names: only (5, 'x') is in the file.
		h := newHistoryHarness(t)
		h.seed(activeVersion(5, "x", "v1", "01:00"), activeVersion(5, "y", "v2", "01:00"))
		h.writeEarliestStart(h.earliestStartFile("5,x," + csvTime("01:30")))
		assert.Equal(t, []historyVersion{
			closedVersion(5, "x", "v1", "01:00", chTime("01:30")),
			activeVersion(5, "y", "v2", "01:00"),
		}, h.rows())
	})

	t.Run("boundary is inclusive and unknown keys are ignored", func(t *testing.T) {
		// id 10: earliest equals the active version's start -> that version is removed, the older closed one stays.
		// id 11: earliest is after every version -> nothing removed, active closed.
		// id 99 is not in the table.
		h := newHistoryHarness(t)
		h.seed(
			closedVersion(10, "a", "old", "01:00", "2024-01-01 01:59:59.999000000"),
			activeVersion(10, "a", "new", "02:00"),
			activeVersion(11, "a", "cur", "03:00"),
		)
		h.writeEarliestStart(h.earliestStartFile("10,a,"+csvTime("02:00"), "11,a,"+csvTime("05:00"), "99,a,"+csvTime("01:00")))
		assert.Equal(t, []historyVersion{
			closedVersion(10, "a", "old", "01:00", "2024-01-01 01:59:59.999000000"),
			closedVersion(11, "a", "cur", "03:00", chTime("05:00")),
		}, h.rows())
	})

	t.Run("rows with NULL active flag are neither removed nor closed", func(t *testing.T) {
		h := newHistoryHarness(t)
		h.seedNullActive(20, "a", "v", "01:00")
		h.writeEarliestStart(h.earliestStartFile("20,a," + csvTime("02:00")))
		assert.Equal(t, []historyVersion{{20, "a", "v", chTime("01:00"), historyMaxEnd, false, 1704067200000}}, h.rows())
	})

	t.Run("files are applied in order", func(t *testing.T) {
		// File 1 closes id 30 at T3; file 2 (T2) then finds nothing active and nothing overlapping.
		// Reversed order would leave _fivetran_end at T2.
		h := newHistoryHarness(t)
		h.seed(activeVersion(30, "a", "v", "01:00"))
		h.writeEarliestStart(h.earliestStartFile("30,a,"+csvTime("03:00")), h.earliestStartFile("30,a,"+csvTime("02:00")))
		assert.Equal(t, []historyVersion{closedVersion(30, "a", "v", "01:00", chTime("03:00"))}, h.rows())
	})

	t.Run("header only file changes nothing", func(t *testing.T) {
		h := newHistoryHarness(t)
		h.seed(activeVersion(40, "a", "v", "01:00"))
		h.writeEarliestStart(h.earliestStartFile())
		assert.Equal(t, []historyVersion{activeVersion(40, "a", "v", "01:00")}, h.rows())
		assert.Equal(t, uint64(0), h.helperTables())
	})

	t.Run("replaying the same file changes nothing", func(t *testing.T) {
		h := newHistoryHarness(t)
		h.seed(activeVersion(45, "a", "v1", "01:00"), activeVersion(45, "a", "v2", "03:00"))
		file := h.earliestStartFile("45,a," + csvTime("02:00"))
		h.writeEarliestStart(file)
		expected := h.rows()
		require.Equal(t, []historyVersion{closedVersion(45, "a", "v1", "01:00", chTime("02:00"))}, expected)
		h.writeEarliestStart(file)
		assert.Equal(t, expected, h.rows())
	})

	t.Run("invalid value fails the batch and leaves no helper table", func(t *testing.T) {
		h := newHistoryHarness(t)
		h.seed(activeVersion(48, "a", "v", "01:00"))
		resp := h.writeEarliestStartResponse(h.earliestStartFile("48,a,not-a-timestamp"))
		require.NotNil(t, resp.GetTask(), "expected a failed response")
		assert.Contains(t, resp.GetTask().GetMessage(), "not-a-timestamp")
		assert.Equal(t, []historyVersion{activeVersion(48, "a", "v", "01:00")}, h.rows())
		assert.Equal(t, uint64(0), h.helperTables())
	})

	t.Run("files larger than the batch sizes are fully applied", func(t *testing.T) {
		restore := []struct {
			flag *uint
			old  uint
		}{{flags.WriteBatchSize, *flags.WriteBatchSize}, {flags.MutationBatchSize, *flags.MutationBatchSize}, {flags.HardDeleteBatchSize, *flags.HardDeleteBatchSize}}
		for _, r := range restore {
			*r.flag = 2
		}
		defer func() {
			for _, r := range restore {
				*r.flag = r.old
			}
		}()
		h := newHistoryHarness(t)
		var seeded []historyVersion
		var lines []string
		var expected []historyVersion
		for id := int64(50); id < 55; id++ {
			seeded = append(seeded, activeVersion(id, "a", "v1", "01:00"), activeVersion(id, "a", "v2", "03:00"))
			lines = append(lines, fmt.Sprintf("%d,a,%s", id, csvTime("02:00")))
			expected = append(expected, closedVersion(id, "a", "v1", "01:00", chTime("02:00")))
		}
		h.seed(seeded...)
		h.writeEarliestStart(h.earliestStartFile(lines...))
		assert.Equal(t, expected, h.rows())
	})
}
