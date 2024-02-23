package types

import pb "fivetran.com/fivetran_sdk/proto"

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

// PrimaryKeysAndMetadataColumns
// In all CSV files, fivetran_sdk.Table column index = CSV column index.
// PrimaryKeys are used when generating select queries, see sql.GetSelectByPrimaryKeysQuery.
// also used for generating mapping keys, see db.GetCSVRowMappingKey and db.GetDatabaseRowMappingKey.
// FivetranSyncedIdx index of the _fivetran_synced column in fivetran_sdk.Table (see db.ToSoftDeletedRow)
// FivetranDeletedIdx index of the _fivetran_deleted column in fivetran_sdk.Table (see db.ToSoftDeletedRow)
type PrimaryKeysAndMetadataColumns struct {
	PrimaryKeys        []*PrimaryKeyColumn
	FivetranSyncedIdx  uint
	FivetranDeletedIdx uint
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
