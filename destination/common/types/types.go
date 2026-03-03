package types

import (
	"reflect"

	pb "fivetran.com/fivetran_sdk/proto"
)

// ColumnDefinition as it is defined or should be defined in ClickHouse
type ColumnDefinition struct {
	Name          string
	Type          string
	Comment       string
	IsPrimaryKey  bool
	DecimalParams *pb.DecimalParams // only for Decimal types, nil otherwise
}

// TableDescription is a description of a ClickHouse table.
// Mapping is ColumnDefinition.Name -> ColumnDefinition (unordered)
// Columns are the same as in Mapping, but ordered, used to preserve column order for CREATE TABLE statement generation
// PrimaryKeys is a convenience list of ColumnDefinition.Name that are primary keys
type TableDescription struct {
	Mapping     map[string]*ColumnDefinition
	Columns     []*ColumnDefinition
	PrimaryKeys []string
}

// PrimaryKeyColumn as it is defined in a Fivetran request or in ClickHouse
type PrimaryKeyColumn struct {
	Index uint
	Name  string
	Type  pb.DataType
}

// FivetranTableMetadata contains metadata about a Fivetran table.
// ColumnsMap extracted from fivetran_sdk.Table.
// FivetranSyncedIdx index of the _fivetran_synced column in fivetran_sdk.Table.
type FivetranTableMetadata struct {
	ColumnsMap        map[string]*pb.Column
	FivetranSyncedIdx uint
}

type AlterTableOpType int

const (
	AlterTableAdd AlterTableOpType = iota
	AlterTableModify
	AlterTableDrop
)

type AlterTableOp struct {
	Op      AlterTableOpType
	Column  string
	Type    *string // nil for AlterTableDrop
	Comment *string // nil for AlterTableDrop
}

// UserGrant represents a row from system.grants table.
type UserGrant struct {
	AccessType string
	Database   *string
	Table      *string
	Column     *string
}

// CSVColumn represents a column in a CSV file with added information from the fivetran_sdk.Table.
// Index = CSV column index.
// TableIndex = ClickHouse table index.
type CSVColumn struct {
	Index        uint
	TableIndex   uint
	Name         string
	Type         pb.DataType
	IsPrimaryKey bool
}

// CSVColumns is an ordered list of CSVColumn, matching the CSV header definition.
// Required to map CSV columns to ClickHouse table columns,
// as CSV files may not to have the same order as the ClickHouse table.
// Example: suppose we have a table in ClickHouse with columns (id Int32, name String, ts DateTime),
// and we receive a CSV with the header (ts, id, name) + fivetran_sdk.Table with the information about the data types.
// Then the resulting CSVColumns would be 0 -> { 2, name, STRING }, 1 -> { 0, id, INT }, 2 -> { 1, ts, NAIVE_DATETIME }.
//
// All = all columns in the CSV file.
// PrimaryKeys = only primary key columns in the CSV file.
//
// See also: MakeCSVColumns.
type CSVColumns struct {
	All         []*CSVColumn
	PrimaryKeys []*CSVColumn
}

// DriverColumn is similar to driver.ColumnType, but it is a struct with extracted values, not an interface;
// additionally, contains the database index.
type DriverColumn struct {
	Index        uint
	Name         string
	ScanType     reflect.Type
	DatabaseType string
}

// DriverColumns is a mapping of driver column names to driver columns.
// Mapping is DriverColumn.Name -> DriverColumn (unordered)
// Columns are the same as in Mapping, but ordered.
type DriverColumns struct {
	Mapping map[string]*DriverColumn
	Columns []*DriverColumn
}

// RemovePrimaryKey removes a primary key column by name from the CSVColumns.PrimaryKeys slice.
// If the column doesn't exist, this is a no-op.
func (c *CSVColumns) RemovePrimaryKey(name string) {
	newKeys := make([]*CSVColumn, 0, len(c.PrimaryKeys))
	for _, col := range c.PrimaryKeys {
		if col.Name != name {
			newKeys = append(newKeys, col)
		}
	}
	c.PrimaryKeys = newKeys
}
