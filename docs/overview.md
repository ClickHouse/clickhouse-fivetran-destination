---
name: ClickHouse Cloud
title: Fivetran destination for ClickHouse Cloud | Configuration and documentation
description: Move your data to ClickHouse Cloud using Fivetran.
menuPosition: 50
---

# ClickHouse Cloud {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}

The quickest and easiest way to get up and running with ClickHouse is to create a new service
in [ClickHouse Cloud](https://clickhouse.cloud).

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to ClickHouse Cloud
> destination and its documentation, contact [ClickHouse Cloud Support](mailto:support@clickhouse.com).

----

## Setup guide

Follow our [setup guide](/docs/destinations/clickhouse-cloud/setup-guide) to configure your Fivetran destination for
ClickHouse Cloud.

----

## Data types mapping

[Fivetran data types](https://fivetran.com/docs/destinations#datatypes) to ClickHouse mapping overview:

| Fivetran type | ClickHouse type                                                                            |
|---------------|--------------------------------------------------------------------------------------------|
| BOOLEAN       | [Bool](https://clickhouse.com/docs/en/sql-reference/data-types/boolean)                    |
| SHORT         | [Int16](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint)                  |
| INT           | [Int32](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint)                  |
| LONG          | [Int64](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint)                  |
| BIGDECIMAL    | [Decimal(P, S)](https://clickhouse.com/docs/en/sql-reference/data-types/decimal)           |
| FLOAT         | [Float32](https://clickhouse.com/docs/en/sql-reference/data-types/float)                   |
| DOUBLE        | [Float64](https://clickhouse.com/docs/en/sql-reference/data-types/float)                   |
| LOCALDATE     | [Date32](https://clickhouse.com/docs/en/sql-reference/data-types/date32)                   |
| LOCALDATETIME | [DateTime64(0, 'UTC')](https://clickhouse.com/docs/en/sql-reference/data-types/datetime64) |
| INSTANT       | [DateTime64(9, 'UTC')](https://clickhouse.com/docs/en/sql-reference/data-types/datetime64) |
| STRING        | [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)                   |
| BINARY        | [String](https://clickhouse.com/docs/en/sql-reference/data-types/string) &ast;             |
| XML           | [String](https://clickhouse.com/docs/en/sql-reference/data-types/string) &ast;             |
| JSON          | [String](https://clickhouse.com/docs/en/sql-reference/data-types/string) &ast;             |

> &ast; NOTE: The ClickHouse [String](https://clickhouse.com/docs/en/sql-reference/data-types/string) type can be used
> to represent an arbitrary set of bytes. The ClickHouse destination adds a column comment to the `JSON`, `BINARY`,
> and `XML` types to indicate the original data type.
> [JSON](https://clickhouse.com/docs/en/sql-reference/data-types/json) data type is not used as it is marked as
> obsolete, and was never recommended for production usage.

## Destination tables

The ClickHouse destination uses
[Replacing](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree) engine type of
[SharedMergeTree](https://clickhouse.com/docs/en/cloud/reference/shared-merge-tree) family
(specifically, `SharedReplacingMergeTree`), versioned by the `_fivetran_synced` column.

Every column except primary (ordering) keys and Fivetran metadata columns is created
as [Nullable(T)](https://clickhouse.com/docs/en/sql-reference/data-types/nullable), where `T` is a
ClickHouse type based on the [data types mapping](#data-types-mapping).

### Single primary key in the source table

For example, source table `users` has a primary key column `id` (`INT`) and a regular column `name` (`STRING`).
The destination table will be defined as follows:

```sql
CREATE TABLE `users`
(
    `id`                Int32,
    `name`              Nullable(String),
    `_fivetran_synced`  DateTime64(9, 'UTC'),
    `_fivetran_deleted` Bool
) ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', _fivetran_synced)
ORDER BY id
SETTINGS index_granularity = 8192
```

In this case, the `id` column is chosen as a table sorting key.

### Multiple primary keys in the source table

If the source table has multiple primary keys, they are used in order of their appearance in the Fivetran source table
definition.

For example, there is a source table `items` with primary key columns `id` (`INT`) and `name` (`STRING`), plus an
additional regular column `description` (`STRING`). The destination table will be defined as follows:

```sql
CREATE TABLE `items`
(
    `id`                Int32,
    `name`              String,
    `description`       Nullable(String),
    `_fivetran_synced`  DateTime64(9, 'UTC'),
    `_fivetran_deleted` Bool
) ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', _fivetran_synced)
ORDER BY (id, name)
SETTINGS index_granularity = 8192
```

In this case, `id` + `name` columns were chosen as table sorting keys.

### No primary keys in the source table

If the source table has no primary keys, a unique identifier will be added by Fivetran as a `_fivetran_id` column.
Consider an `events` table that only has the `event` (`STRING`) and `timestamp` (`LOCALDATETIME`) columns in the source.
The destination table in that case is as follows:

```sql
CREATE TABLE events
(
    `event`             Nullable(String),
    `timestamp`         Nullable(DateTime),
    `_fivetran_id`      String,
    `_fivetran_synced`  DateTime64(9, 'UTC'),
    `_fivetran_deleted` Bool
) ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', _fivetran_synced)
ORDER BY _fivetran_id
SETTINGS index_granularity = 8192
```

Since `_fivetran_id` is unique and there are no other primary key options, it is used as a table sorting key.

### Selecting the latest version of the data without duplicates

`SharedReplacingMergeTree` performs background data deduplication
[only during merges at an unknown time](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree).
However, selecting the latest version of the data without duplicates ad-hoc is possible with the `FINAL` keyword and
[select_sequential_consistency](https://clickhouse.com/docs/en/operations/settings/settings#select_sequential_consistency)
setting:

```sql
SELECT *
FROM example FINAL LIMIT 1000 
SETTINGS select_sequential_consistency = 1;
```

### Retries on network failures

The ClickHouse destination retries transient network errors using the exponential backoff algorithm.
This is safe even when the destination inserts the data, as any potential duplicates are handled by
the `SharedReplacingMergeTree` table engine, either during background merges,
or when querying the data with `SELECT FINAL`.

## Preview limitations

- Adding, removing or modifying primary key columns is not supported yet.
- The custom ClickHouse settings configuration (for example, for the `CREATE TABLE` statements) is not supported yet.