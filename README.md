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

## Destination table

The destination app will create a ClickHouse table
using [ReplacingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
engine versioned by `_fivetran_synced` column. Every column except primary keys and Fivetran metadata columns will be of
type `Nullable(T)`.

For example, if the source table had primary key column `id` and a regular column `name`, the destination table would
look like this:

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

If the source table had no primary keys, it will be added by Fivetran as a `_fivetran_id` column. Consider `events`
table that only has `timestamp` and `event` columns in the source. The destination table will look like this:

```sql
CREATE TABLE events
(
    `timestamp`         Nullable(DateTime),
    `event`             Nullable(String),
    `_fivetran_id`      String,
    `_fivetran_synced`  DateTime64(9, 'UTC'),
    `_fivetran_deleted` Bool
)
```

ReplacingMergeTree performs background data deduplication only during merges at an unknown time; however, selecting the
data without duplicates is possible with `FINAL` keyword:

```sql
SELECT *
FROM example FINAL LIMIT 1000
```

## Contributing

See our [contributing guide](CONTRIBUTING.md).
