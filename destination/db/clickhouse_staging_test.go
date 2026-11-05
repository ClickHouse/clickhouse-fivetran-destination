package db

import (
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stagingName(t *testing.T, schema, table, operation string) string {
	t.Helper()
	staging, err := NewStagingTable(schema, table, operation, nil, nil)
	require.NoError(t, err)
	return staging.name
}

func TestNewStagingTableName(t *testing.T) {
	pattern := regexp.MustCompile(`^(.+)_fivetran_tmp_delete_\d{13}$`)
	const schema = "fivetran_verify" // 15 bytes: ClickHouse accepts names of at most 198 here
	limit := maxTableNameLength - len(schema)

	assert.Equal(t, "users", pattern.FindStringSubmatch(stagingName(t, schema, "users", "delete"))[1])

	// the longest destination name ClickHouse accepts in this schema still fits with the suffix
	long := strings.Repeat("n", limit)
	name := stagingName(t, schema, long, "earliest_start")
	assert.LessOrEqual(t, len(name), limit)
	base := regexp.MustCompile(`^(.+)_fivetran_tmp_earliest_start_\d{13}$`).FindStringSubmatch(name)[1]
	assert.True(t, strings.HasPrefix(long, base[:len(base)-9]), "keeps a prefix of the table name")

	// two long names sharing a prefix get different staging names
	other := stagingName(t, schema, long+"x", "earliest_start")
	assert.NotEqual(t, name[:len(name)-14], other[:len(other)-14])

	// a longer schema name shortens the staging name accordingly; a schema near the limit leaves only the hash
	assert.LessOrEqual(t, len(stagingName(t, strings.Repeat("s", 60), long, "delete")), maxTableNameLength-60)
	assert.Regexp(t, `^[0-9a-f]{8}_fivetran_tmp_delete_\d{13}$`, stagingName(t, strings.Repeat("s", 200), long, "delete"))

	// a multi-byte character straddling the cut is dropped rather than split
	unicode := strings.Repeat("é", 120)
	require.Greater(t, len(unicode), limit-len("_fivetran_tmp_delete_")-13)
	unicodeName := stagingName(t, schema, unicode, "delete")
	assert.True(t, utf8.ValidString(unicodeName))
	assert.LessOrEqual(t, len(unicodeName), limit)
}
