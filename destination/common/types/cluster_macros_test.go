package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToClusterMacros(t *testing.T) {
	macros, err := ToClusterMacros(map[string]string{
		"cluster": "foo",
		"replica": "bar",
		"shard":   "qaz",
	})
	assert.NoError(t, err)
	assert.Equal(t, macros, &ClusterMacros{
		Cluster: "foo",
		Replica: "bar",
		Shard:   "qaz",
	})

	_, err = ToClusterMacros(map[string]string{
		"cluster": "foo",
		"replica": "bar",
		"_ignore": "_",
	})
	assert.ErrorContains(t, err, "macro shard is missing")

	_, err = ToClusterMacros(map[string]string{
		"cluster": "foo",
		"shard":   "qaz",
		"_ignore": "_",
	})
	assert.ErrorContains(t, err, "macro replica is missing")

	_, err = ToClusterMacros(map[string]string{
		"replica": "bar",
		"shard":   "qaz",
		"_ignore": "_",
	})
	assert.ErrorContains(t, err, "macro cluster is missing")
}
