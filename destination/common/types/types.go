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

// TableDescription
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

// FivetranTableMetadata
// PrimaryKeys are used when generating select queries, see sql.GetSelectByPrimaryKeysQuery.
// also used for generating mapping keys, see db.GetCSVRowMappingKey and db.GetDatabaseRowMappingKey.
// FivetranSyncedIdx index of the _fivetran_synced column in fivetran_sdk.Table (see db.ToSoftDeletedRow)
// FivetranDeletedIdx index of the _fivetran_deleted column in fivetran_sdk.Table (see db.ToSoftDeletedRow)
// NB: allowed to be -1, e.g. when _fivetran_deleted column is not present in the table definition immediately.
type FivetranTableMetadata struct {
	PrimaryKeys        []*PrimaryKeyColumn
	ColumnsMap         map[string]*pb.Column
	FivetranSyncedIdx  uint
	FivetranDeletedIdx int
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
// See also: MakeCSVColumns.
type CSVColumns []*CSVColumn

// DriverColumn is similar to driver.ColumnType, but it is a struct with extracted values, not an interface;
// additionally, contains the database index.
type DriverColumn struct {
	Index        uint
	Name         string
	ScanType     reflect.Type
	DatabaseType string
}
