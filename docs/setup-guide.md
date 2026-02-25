---
name: Setup Guide
title: Fivetran destination for ClickHouse Cloud Setup Guide
description: Follow the guide to set up ClickHouse Cloud as a destination.
---

# ClickHouse Cloud Setup Guide {% badge text="Partner-Built" /%} {% availabilityBadge connector="clickhouse" /%}

Follow our setup guide to configure your Fivetran destination for ClickHouse Cloud.

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to ClickHouse Cloud
> destination and its documentation, contact [ClickHouse Cloud Support](mailto:support@clickhouse.com).

---

## Prerequisites

To connect Fivetran to ClickHouse Cloud, you will need the following:

- A Fivetran account with
  [permission to add destinations](/docs/using-fivetran/fivetran-dashboard/account-settings/role-based-access-control#legacyandnewrbacmodel).
- A ClickHouse Cloud service. You can follow the [Quick Start Guide](https://clickhouse.com/docs/en/cloud-quick-start).
  When the service is created, make sure to copy the `default` user credentials - the password will be shown only once.
- (Recommended) Consider not using the `default` user; instead, create a dedicated one to use it with this Fivetran
  destination only. The following commands, executed with the `default` user, will create a new `fivetran_user` with the
  required privileges.

   ```sql
   CREATE USER fivetran_user IDENTIFIED BY '<password>'; -- use a secure password generator

   GRANT CURRENT GRANTS ON *.* TO fivetran_user;
   ```

  Additionally, you can revoke access to certain databases from the `fivetran_user`.
  For example, by executing the following statement, we restrict access to the `default` database:

  ```sql
  REVOKE ALL ON default.* FROM fivetran_user;
  ```

  You can execute these statements in the ClickHouse SQL console. On the navigation menu, select your service 
  on the services list and then click **+** to add a new query.

  ![SQL Console](./_assets/clickhouse_setup_guide3.png)

  Paste the SQL statements into the query editor, replace the `<password>` placeholder with a password of your choice,
  and press the **Run** button.

  ![Executing the statements](./_assets/clickhouse_setup_guide4.png)

  Now, you should be able to use the `fivetran_user` credentials in the destination configuration.

---

## Find connection details

You can find the hostname of your service in the ClickHouse console. On the navigation menu, select your service 
and then click **Connect**.

![Connect button](./_assets/clickhouse_setup_guide1.png)

In the connection window, select **Native**. The hostname required for the destination configuration matches the `--host`
argument for the CLI client. It is defined with the following format: `<service>.<region>.<provider>.clickhouse.cloud`.

![Hostname](./_assets/clickhouse_setup_guide2.png)

The port required for the destination configuration is ClickHouse Cloud native secure port, which is `9440` for most
instances.

---

## Destination configuration

1. Log in to your [Fivetran account](https://fivetran.com/login).
2. Go to the **Destinations** page and click **Add destination**.
3. Choose a **Destination name** of your choice.
4. Click **Add**.
5. Select **ClickHouse Cloud** as the destination type.
6. Enter your ClickHouse Cloud service hostname.
7. Enter your ClickHouse Cloud service port.
8. Enter the credentials of the user.
9. Click **Save & Test**.

Fivetran will run the connectivity check with your ClickHouse Cloud service using the provided credentials. If it
succeeded, you can start ingesting the data into your ClickHouse Cloud service using Fivetran connectors.

In addition, Fivetran automatically configures a [Fivetran Platform Connector](/docs/logs/fivetran-platform) to transfer
the connection logs and account metadata to a schema in this destination. The Fivetran Platform Connector enables you to
monitor your connections, track your usage, and audit changes. The Fivetran Platform Connector sends all these details at the destination
level.

> IMPORTANT: If you are an Account Administrator, you can manually add the Fivetran Platform Connector on an account level so that it syncs all the metadata and logs for all the destinations in your account to a single destination. If an account-level Fivetran Platform Connector is already configured in a destination in your Fivetran account, then we don't add destination-level Fivetran Platform Connectors to the new destinations you create.


## Advanced Configuration

The ClickHouse Cloud destination supports an optional JSON configuration file for advanced use cases.
This file allows you to fine-tune destination behavior, override ClickHouse query-level settings,
and configure individual tables with custom sorting keys or table-level settings.

> NOTE: This configuration is entirely optional. If no file is uploaded, the destination uses
> sensible defaults that work well for most use cases.

---

### Uploading the configuration file

During [destination setup](/docs/destinations/clickhouse/setup-guide), you can upload a `.json` file
in the **Advanced Configuration** field. The file must be valid JSON and conform to the schema described below.

If you need to modify the configuration after the initial setup, you can edit the destination settings
in the Fivetran dashboard and upload an updated file.

---

### Configuration file schema

The configuration file has three top-level sections, all of which are optional:

```json
{
  "destination_settings": { ... },
  "clickhouse_query_settings": { ... },
  "tables": { ... }
}
```

#### `destination_settings`

Controls the internal behavior of the ClickHouse destination connector itself.
These settings affect how the connector processes data before sending it to ClickHouse.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `write_batch_size` | integer | `100000` | Number of rows per batch for insert, update, and replace operations. |
| `select_batch_size` | integer | `1500` | Number of rows per batch for SELECT queries used during updates. |
| `hard_delete_batch_size` | integer | `1500` | Number of rows per batch for hard delete operations. |
| `max_parallel_selects` | integer | `10` | Maximum number of concurrent SELECT queries during batch processing. |
| `max_idle_connections` | integer | `5` | Maximum number of idle connections in the ClickHouse connection pool. |
| `max_open_connections` | integer | `10` | Maximum number of open connections in the ClickHouse connection pool. |
| `request_timeout_seconds` | integer | `300` | Timeout in seconds for individual ClickHouse requests. |

Example:

```json
{
  "destination_settings": {
    "write_batch_size": 500000,
    "max_parallel_selects": 20
  }
}
```

#### `clickhouse_query_settings`

A free-form map of [ClickHouse session-level settings](https://clickhouse.com/docs/en/operations/settings/settings)
that are applied to every query the destination executes. Keys must be valid ClickHouse setting names.
Values must be strings, integers, or booleans, matching the types expected by ClickHouse.

These settings override the destination's built-in defaults. The following settings are applied by default
and can be overridden:

| Setting | Default | Description |
|---------|---------|-------------|
| `date_time_input_format` | `"best_effort"` | Parsing mode for DateTime values from CSV batch files. |
| `alter_sync` | `3` | Synchronization mode for ALTER queries across replicas (ClickHouse Cloud only). |
| `mutations_sync` | `3` | Synchronization mode for mutations across replicas (ClickHouse Cloud only). |
| `select_sequential_consistency` | `1` | Ensures sequential consistency for SELECT queries (ClickHouse Cloud only). |

> WARNING: Changing these defaults may cause data inconsistency or sync failures.
> Only override them if you understand the implications for your ClickHouse Cloud deployment.

Example:

```json
{
  "clickhouse_query_settings": {
    "max_insert_threads": 4,
    "insert_quorum": 2
  }
}
```

#### `tables`

Per-table configuration for ClickHouse `CREATE TABLE` statements. Each key is a fully qualified table name
in the format `schema_name.table_name`, matching the schema and table names as they appear in Fivetran.

Any table not listed here uses the default settings. Table-specific settings override the defaults
only for that particular table.

All tables use the `ReplacingMergeTree(_fivetran_synced)` engine. The engine cannot be overridden.

Each table entry supports the following settings:

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `order_by` | array of strings | *(primary keys from source)* | Column names for the `ORDER BY` clause. |
| `settings` | object | *(none)* | Key-value map of [ClickHouse table-level SETTINGS](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#settings). Keys must be valid setting names; values can be strings, integers, or booleans. |

##### ORDER BY behavior

- If `order_by` is **not specified**, the primary key columns from the Fivetran source table are used.
  This is the recommended default, as Fivetran determines the correct primary keys for each source table.
- If `order_by` **is specified**, the listed columns are used instead. All listed columns must exist
  in the table. If a column does not exist, the `CREATE TABLE` operation will fail.

##### Example

```json
{
  "tables": {
    "salesforce.accounts": {
      "order_by": ["created_at", "account_id"],
      "settings": { "index_granularity": 2048 }
    },
    "salesforce.events": {
      "order_by": ["event_timestamp", "event_id"],
      "settings": { "index_granularity": 1024, "storage_policy": "hot_cold" }
    }
  }
}
```

In this example:

- `salesforce.accounts` uses a custom sorting key and index granularity.
- `salesforce.events` uses a custom sorting key and storage policy.
- All other tables use the default Fivetran-provided primary keys as the sorting key.

---

### Full example

```json
{
  "destination_settings": {
    "write_batch_size": 200000,
    "max_parallel_selects": 5,
    "request_timeout_seconds": 600
  },
  "clickhouse_query_settings": {
    "max_insert_threads": 4
  },
  "tables": {
    "my_schema.large_events": {
      "order_by": ["event_date", "event_id"],
      "settings": { "index_granularity": 2048 }
    },
    "my_schema.users": {
      "settings": { "index_granularity": 512 }
    }
  }
}
```

---

### Limitations

- The configuration file applies to all syncs for the destination. It cannot vary per sync or per connector.
- Table names in the `tables` section must exactly match the `schema_name.table_name`
  as they appear in Fivetran (case-sensitive).
- If a table listed in `tables` is not synced by any connector, its configuration is ignored.
- ClickHouse query settings in `clickhouse_query_settings` are not validated by the destination;
  invalid setting names or values will cause query failures reported during sync.
- The maximum file size allowed for the configuration file is 1 MB.
