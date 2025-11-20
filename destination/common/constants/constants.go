package constants

const (
	FivetranID      = "_fivetran_id"
	FivetranSynced  = "_fivetran_synced"
	FivetranDeleted = "_fivetran_deleted"
	FivetranActive  = "_fivetran_active"
	FivetranStart   = "_fivetran_start"
	FivetranEnd     = "_fivetran_end"
)

const (
	XMLColumnComment       = "XML"
	JSONColumnComment      = "JSON"
	BinaryColumnComment    = "BINARY"
	NaiveTimeColumnComment = "NAIVE_TIME"
)

const (
	String      = "String"
	Bool        = "Bool"
	Date        = "Date32"               // Date32 has a wider range than a regular Date
	DateTime    = "DateTime64(0, 'UTC')" // DateTime64(0, 'UTC') has a wider range than a regular DateTime
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
	UTCDateTimeFormat   = "2006-01-02T15:04:05.9Z" // allows arbitrary timestamp precision from 0 to 9
	NaiveDateTimeFormat = "2006-01-02T15:04:05"
	NaiveDateFormat     = "2006-01-02"
)
