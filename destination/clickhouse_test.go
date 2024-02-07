package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetFullTableName(t *testing.T) {
	fullName, err := GetFullTableName("foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "`foo`.`bar`", fullName)

	fullName, err = GetFullTableName("", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "`bar`", fullName)

	_, err = GetFullTableName("foo", "")
	assert.ErrorContains(t, err, "table name is empty")
}
