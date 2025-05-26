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
