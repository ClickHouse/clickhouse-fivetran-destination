---
name: Setup Guide
title: Fivetran destination for ClickHouse Cloud Setup Guide
description: Follow the guide to set up ClickHouse Cloud as a destination.
hidden: true
---

# ClickHouse Cloud Setup Guide {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}

Follow our setup guide to configure your Fivetran destination for ClickHouse Cloud.

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to ClickHouse Cloud destination and its documentation, contact [ClickHouse Cloud Support](mailto:support@clickhouse.com).

---

## Prerequisites

To connect Fivetran to ClickHouse Cloud, you will need the following:

- A Fivetran account with [permission to add destinations](/docs/using-fivetran/fivetran-dashboard/account-management/role-based-access-control#legacyandnewrbacmodel).
- A ClickHouse Cloud service. You can follow the [Quick Start Guide](https://clickhouse.com/docs/en/cloud-quick-start).
- (Recommended) Consider not using the `default` user and database; instead, create dedicated ones to use it with this Fivetran destination only. The following commands will create a new database `fivetran` and a user `fivetran_user` with the required privileges in the destination database.

   ```sql
   CREATE DATABASE fivetran;

   CREATE USER fivetran_user IDENTIFIED BY '<password>'; -- use a secure password generator

   GRANT 
     SHOW TABLES, SHOW COLUMNS, SELECT, INSERT, CREATE TABLE, DROP TABLE, TRUNCATE TABLE,
     ALTER UPDATE, ALTER DELETE, ALTER ADD COLUMN, ALTER MODIFY COLUMN, ALTER DROP COLUMN 
   ON fivetran.* TO fivetran_user;
   ```

  See the following resources to learn more about user privileges:
  - [SHOW TABLES, SHOW COLUMNS](https://clickhouse.com/docs/en/sql-reference/statements/grant#show)
  - [CREATE TABLE](https://clickhouse.com/docs/en/sql-reference/statements/grant#create)
  - [SELECT](https://clickhouse.com/docs/en/sql-reference/statements/grant#select)
  - [INSERT](https://clickhouse.com/docs/en/sql-reference/statements/grant#insert)
  - [DROP TABLE](https://clickhouse.com/docs/en/sql-reference/statements/grant#drop)
  - [TRUNCATE TABLE](https://clickhouse.com/docs/en/sql-reference/statements/grant#truncate)
  - [ALTER](https://clickhouse.com/docs/en/sql-reference/statements/grant#alter)

---

## Destination configuration

1. Log in to your Fivetran account.
2. Go to the [**Destinations** page](https://fivetran.com/dashboard/destinations), and then click **+ Add Destination**.
3. Choose a **Destination name** of your choice.
4. Click **Add**.
5. Select **ClickHouse** as the destination type.
6. Enter your ClickHouse Cloud service hostname. It should be in the following format:  `service.clickhouse.cloud:9440`. Note that it uses the Native protocol secure port (9440).
7. Enter the credentials of the user and the destination database.
8. Click **Save & Test**.

Fivetran will run the connectivity check with your ClickHouse Cloud service using the provided credentials. If it succeeded, you can start ingesting the data into your ClickHouse Cloud service using Fivetran connectors.