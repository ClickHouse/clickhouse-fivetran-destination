package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetCSVRowMappingKey(t *testing.T) {
	row := CSVRow{"true", "false", "42", "100.5", "2021-03-04T22:44:22.123456789Z", "2023-05-07T18:22:44", "2019-12-15", "test"}

	// Serialization of a single PK to a mapping key (assuming one column is defined as a PK in Fivetran)
	singlePrimaryKeyArgs := []struct {
		*PrimaryKeyColumn
		string
	}{
		{&PrimaryKeyColumn{Name: "b1", Index: 0}, "b1:true"},
		{&PrimaryKeyColumn{Name: "b2", Index: 1}, "b2:false"},
		{&PrimaryKeyColumn{Name: "i32", Index: 2}, "i32:42"},
		{&PrimaryKeyColumn{Name: "f32", Index: 3}, "f32:100.5"},
		{&PrimaryKeyColumn{Name: "dt_utc", Index: 4}, "dt_utc:2021-03-04T22:44:22.123456789Z"},
		{&PrimaryKeyColumn{Name: "dt", Index: 5}, "dt:2023-05-07T18:22:44"},
		{&PrimaryKeyColumn{Name: "d", Index: 6}, "d:2019-12-15"},
		{&PrimaryKeyColumn{Name: "s", Index: 7}, "s:test"},
	}
	for i, arg := range singlePrimaryKeyArgs {
		key, err := GetCSVRowMappingKey(row, []*PrimaryKeyColumn{arg.PrimaryKeyColumn})
		assert.NoError(t, err, "Expected no error for idx %d with key %s", i, arg.string)
		assert.Equal(t, arg.string, key, "Expected key to be %s for idx %d", arg.string, i)
	}

	// Serialization of multiple PKs to a mapping key (assuming two columns are defined as PKs in Fivetran)
	multiplePrimaryKeyArgs := []struct {
		pkCols []*PrimaryKeyColumn
		key    string
	}{
		{pkCols: []*PrimaryKeyColumn{{Name: "b1", Index: 0}, {Name: "b2", Index: 1}}, key: "b1:true,b2:false"},
		{pkCols: []*PrimaryKeyColumn{{Name: "i32", Index: 2}, {Name: "f32", Index: 3}}, key: "i32:42,f32:100.5"},
		{pkCols: []*PrimaryKeyColumn{{Name: "dt_utc", Index: 4}, {Name: "dt", Index: 5}}, key: "dt_utc:2021-03-04T22:44:22.123456789Z,dt:2023-05-07T18:22:44"},
		{pkCols: []*PrimaryKeyColumn{{Name: "d", Index: 6}, {Name: "s", Index: 7}}, key: "d:2019-12-15,s:test"},
	}
	for i, arg := range multiplePrimaryKeyArgs {
		key, err := GetCSVRowMappingKey(row, arg.pkCols)
		assert.NoError(t, err, "Expected no error for idx %d with key %s", i, arg.key)
		assert.Equal(t, arg.key, key, "Expected key to be %s for idx %d", arg.key, i)
	}

	_, err := GetCSVRowMappingKey(row, nil)
	assert.ErrorContains(t, err, "expected non-empty list of primary keys columns")
}

var config = map[string]string{
	"host":     "localhost",
	"port":     "9000",
	"username": "default",
	"password": "",
}
