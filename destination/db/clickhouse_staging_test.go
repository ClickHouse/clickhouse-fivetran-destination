package db

import (
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStagingTableName(t *testing.T) {
	pattern := regexp.MustCompile(`^(.+)_fivetran_tmp_delete_\d{13}$`)
	const schema = "fivetran_verify" // 15 bytes: ClickHouse accepts names of at most 198 here
	limit := maxTableNameLength - len(schema)

	short := stagingTableName(schema, "users", "delete")
	assert.Equal(t, "users", pattern.FindStringSubmatch(short)[1])

	// the longest destination name self-hosted ClickHouse accepts in this schema still fits with the suffix
	long := strings.Repeat("n", limit)
	name := stagingTableName(schema, long, "earliest_start")
	assert.LessOrEqual(t, len(name), limit)
	base := regexp.MustCompile(`^(.+)_fivetran_tmp_earliest_start_\d{13}$`).FindStringSubmatch(name)[1]
	assert.True(t, strings.HasPrefix(long, base[:len(base)-9]), "keeps a prefix of the table name")

	// two long names sharing a prefix get different staging names
	other := stagingTableName(schema, long+"x", "earliest_start")
	assert.NotEqual(t, name[:len(name)-14], other[:len(other)-14])

	// a longer schema name shortens the staging name accordingly; a schema near the limit leaves only the hash
	assert.LessOrEqual(t, len(stagingTableName(strings.Repeat("s", 60), long, "delete")), maxTableNameLength-60)
	assert.Regexp(t, `^[0-9a-f]{8}_fivetran_tmp_delete_\d{13}$`, stagingTableName(strings.Repeat("s", 200), long, "delete"))

	// a multi-byte character straddling the cut is dropped rather than split
	unicode := strings.Repeat("é", 120)
	require.Greater(t, len(unicode), limit-len("_fivetran_tmp_delete_")-13)
	unicodeName := stagingTableName(schema, unicode, "delete")
	assert.True(t, utf8.ValidString(unicodeName))
	assert.LessOrEqual(t, len(unicodeName), limit)
}
