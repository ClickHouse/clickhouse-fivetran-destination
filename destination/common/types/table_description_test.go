package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeTableDescription(t *testing.T) {
	description := MakeTableDescription([]*ColumnDefinition{})
	assert.Equal(t, description, &TableDescription{})

	col1 := &ColumnDefinition{Name: "id", Type: "Int32", IsPrimaryKey: true}
	col2 := &ColumnDefinition{Name: "name", Type: "String", IsPrimaryKey: false}
	col3 := &ColumnDefinition{Name: "age", Type: "Int32", IsPrimaryKey: false}

	description = MakeTableDescription([]*ColumnDefinition{col1, col2, col3})
	assert.Equal(t, description, &TableDescription{
		Mapping:     map[string]*ColumnDefinition{"id": col1, "name": col2, "age": col3},
		Columns:     []*ColumnDefinition{col1, col2, col3},
		PrimaryKeys: []string{"id"},
	})
}
