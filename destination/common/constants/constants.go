package constants

const (
	FivetranID      = "_fivetran_id"
	FivetranSynced  = "_fivetran_synced"
	FivetranDeleted = "_fivetran_deleted"
)

const (
	XMLColumnComment    = "XML"
	JSONColumnComment   = "JSON"
	BinaryColumnComment = "BINARY"
)

const (
	String      = "String"
	Bool        = "Bool"
	Date        = "Date"
	DateTime    = "DateTime"
	DateTimeUTC = "DateTime64(9, 'UTC')"
	Decimal     = "Decimal"
	Int16       = "Int16"
	Int32       = "Int32"
	Int64       = "Int64"
	Float32     = "Float32"
	Float64     = "Float64"
	Nullable    = "Nullable"
)

const MaxDecimalPrecision = 76

const (
	MacroCluster = "cluster"
	MacroReplica = "replica"
	MacroShard   = "shard"
)
