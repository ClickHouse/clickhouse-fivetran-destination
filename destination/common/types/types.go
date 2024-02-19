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

// ClusterMacros values are required for on-premise cluster deployments to generate correct CREATE TABLE statements.
type ClusterMacros struct {
	Cluster string
	Replica string
	Shard   string
}

// InsertQuorumSettings indicates if insert_quorum ClickHouse setting should be applied to inserts and its value.
type InsertQuorumSettings struct {
	Enabled bool
	Value   uint64
}
