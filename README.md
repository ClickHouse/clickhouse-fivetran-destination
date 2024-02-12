<p align="center">
<img src="https://assets-global.website-files.com/6130fa1501794ed4d11867ba/65a87d992f467bd9ad9795a4_blue-logo-only.svg" height="200px" align="center">
<img src=".static/logo.svg" width="200px" align="center">
<h1 align="center">ClickHouse Fivetran Destination</h1>
</p>
<br/>
<p align="center">
<a href="https://github.com/ClickHouse/clickhouse-fivetran-destination/actions/workflows/tests.yml">
<img src="https://github.com/ClickHouse/clickhouse-fivetran-destination/actions/workflows/tests.yml/badge.svg?branch=main">
</a>
</p>

## About

[ClickHouse](https://clickhouse.com) ([GitHub](https://github.com/ClickHouse/ClickHouse)) database destination
for [Fivetran](https://fivetran.com) automated data movement platform, based on
the [Fivetran Partner SDK](https://github.com/fivetran/fivetran_sdk).

## Data types mapping

| Fivetran type  | ClickHouse type                                                                            |
|----------------|--------------------------------------------------------------------------------------------|
| BOOLEAN        | [Bool](https://clickhouse.com/docs/en/sql-reference/data-types/boolean)                    |
| SHORT          | [Int16](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint)                  |
| INT            | [Int32](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint)                  |
| LONG           | [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint)                  |
| DECIMAL        | [Decimal(P, S)](https://clickhouse.com/docs/en/sql-reference/data-types/decimal)           |
| FLOAT          | [Float32](https://clickhouse.com/docs/en/sql-reference/data-types/float)                   |
| DOUBLE         | [Float64](https://clickhouse.com/docs/en/sql-reference/data-types/float)                   |
| NAIVE_DATE     | [Date](https://clickhouse.com/docs/en/sql-reference/data-types/date)                       |
| NAIVE_DATETIME | [DateTime](https://clickhouse.com/docs/en/sql-reference/data-types/datetime)               |
| UTC_DATETIME   | [DateTime64(9, 'UTC')](https://clickhouse.com/docs/en/sql-reference/data-types/datetime64) |
| XML            | [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)                   |
| STRING         | [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)                   |
| JSON           | [JSON](https://clickhouse.com/docs/en/sql-reference/data-types/json) &ast;                 |
| BINARY         | Not supported &ast;&ast;                                                                   |

&ast; The destination app automatically uses the required `allow_experimental_object_type = 1` setting.

&ast;&ast; Though ClickHouse supports binary data
(using [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)
or [FixedString(N)](https://clickhouse.com/docs/en/sql-reference/data-types/fixedstring) types), the coercion to the
STRING type is currently not supported by Fivetran.

## Destination table

The destination app will create a ClickHouse table
using [ReplacingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
engine versioned by `_fivetran_synced` column. Every column except primary keys and Fivetran metadata columns will be of
type `Nullable(T)`.

### Single primary key in the source table

For example, if the source table has primary key column `id` (Integer) and a regular column `name` (String), the
destination table will look like this:

```sql
CREATE TABLE example
(
    `id`                Int32,
    `name`              Nullable(String),
    `_fivetran_synced`  DateTime64(9, 'UTC'),
    `_fivetran_deleted` Bool
) ENGINE = ReplacingMergeTree(_fivetran_synced)
ORDER BY id
SETTINGS index_granularity = 8192
```

In this case, the `id` column is chosen as a table sorting key.

### Multiple primary keys in the source table

If the source table has multiple primary keys, they will be used in order of their appearance in the Fivetran table
definition.

For example, with primary key columns `id` (Integer) and `name` (String), and an
additional regular column `description` (String), the destination table will look as follows:

```sql
CREATE TABLE example
(
    `id`                Int32,
    `name`              String,
    `description`       Nullable(String),
    `_fivetran_synced`  DateTime64(9, 'UTC'),
    `_fivetran_deleted` Bool
) ENGINE = ReplacingMergeTree(_fivetran_synced)
ORDER BY (id, name)
SETTINGS index_granularity = 8192
```

In this case, `id` + `name` columns were chosen as table sorting keys.

### No primary keys in the source table

If the source table has no primary keys, a unique identifier will be added by Fivetran as a `_fivetran_id` column.
Consider `events` table that only has `event` (String) and `timestamp` (DateTime) columns in the source. The destination
table will look like this:

```sql
CREATE TABLE events
(
    `event`             Nullable(String),
    `timestamp`         Nullable(DateTime),
    `_fivetran_id`      String,
    `_fivetran_synced`  DateTime64(9, 'UTC'),
    `_fivetran_deleted` Bool
) ENGINE = ReplacingMergeTree(_fivetran_synced)
ORDER BY _fivetran_id
SETTINGS index_granularity = 8192
```

Since `_fivetran_id` is unique and there are no other primary key options, it is used as a table sorting key.

### Selecting the data without duplicates

ReplacingMergeTree performs background data deduplication only during merges at an unknown time; however, selecting the
data without duplicates ad-hoc is possible with `FINAL` keyword:

```sql
SELECT *
FROM example FINAL LIMIT 1000
```

## Contributing

See our [contributing guide](CONTRIBUTING.md).
