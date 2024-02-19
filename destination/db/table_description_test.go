package db

import (
	"testing"

	"fivetran.com/fivetran_sdk/destination/common/types"
	"github.com/stretchr/testify/assert"
)

func TestMakeTableDescription(t *testing.T) {
	description := MakeTableDescription([]*types.ColumnDefinition{})
	assert.Equal(t, description, &types.TableDescription{})

	col1 := &types.ColumnDefinition{Name: "id", Type: "Int32", IsPrimaryKey: true}
	col2 := &types.ColumnDefinition{Name: "name", Type: "String", IsPrimaryKey: false}
	col3 := &types.ColumnDefinition{Name: "age", Type: "Int32", IsPrimaryKey: false}

	description = MakeTableDescription([]*types.ColumnDefinition{col1, col2, col3})
	assert.Equal(t, description, &types.TableDescription{
		Mapping:     map[string]*types.ColumnDefinition{"id": col1, "name": col2, "age": col3},
		Columns:     []*types.ColumnDefinition{col1, col2, col3},
		PrimaryKeys: []string{"id"},
	})
}
