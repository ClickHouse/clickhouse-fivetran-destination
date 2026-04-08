# Agent Guidelines

## Project overview

ClickHouse destination connector for the Fivetran data movement platform. It implements a gRPC server based on the [Fivetran Partner SDK](https://github.com/fivetran/fivetran_sdk) that receives data from Fivetran and writes it to ClickHouse Cloud.

**Tech stack:** Go 1.22+, gRPC (protobuf), ClickHouse (SharedReplacingMergeTree), Docker, Make.

See [README.md](./README.md) for documentation links and [CONTRIBUTING.md](./CONTRIBUTING.md) for full development setup, testing, and build instructions.

## Key references

- [Fivetran Partner SDK](https://github.com/fivetran/fivetran_partner_sdk) — the authoritative reference for building this destination connector. Contains proto definitions (`common.proto`, `destination_sdk.proto`), the SDK development guide, doc templates, and examples. Always consult this repo when verifying data type mappings, gRPC contract details, or SDK behavior expectations.
- [Schema Migration Helper Guide](https://github.com/fivetran/fivetran_partner_sdk/blob/f61fa5186477d25fb3564233773e0123e73c29bd/schema-migration-helper-service.md) — documents the `Migrate` RPC and all schema migration operations (DDL, DML, sync mode transitions, history mode). Refer to this when implementing or debugging migration operations.
- [ClickHouse ALTER TABLE docs](https://clickhouse.com/docs/sql-reference/statements/alter/column) — reference for column manipulation operations (ADD, DROP, RENAME, MODIFY COLUMN). Important: RENAME COLUMN and ADD COLUMN are instant metadata-only operations, while UPDATE is a heavyweight mutation.

## Architecture

**Go module:** `fivetran.com/fivetran_sdk`

The codebase lives under `destination/` and is organized as:

- `cmd/` — entry point; starts the gRPC server on port 50052
- `service/` — gRPC service implementation (`Server` struct implements `DestinationConnectorServer`). RPC handlers are in `server.go`; the `Migrate` RPC handler is in `migrate.go`
- `db/` — ClickHouse database operations (connections, queries, mutations). Migration-specific DB methods (e.g., `MigrateCopyTable`, `MigrateSoftDeleteToHistory`) are in `clickhouse.go`
- `db/sql/` — SQL query building
- `db/config/` — connection configuration parsing
- `db/values/` — value conversion for ClickHouse types
- `common/` — shared utilities: `flags`, `log`, `constants`, `data_types`, `types`, `retry`, `benchmark`, `csv`
- `encryption/aes/` — AES encryption for file handling

**Proto definitions** are in `proto/` (downloaded from the Fivetran SDK, not authored here). Generated Go code also lives in `proto/`.

## Common commands

All commands are explained in [CONTRIBUTING.md](./CONTRIBUTING.md) and defined in the [Makefile](./Makefile). Before wanting to execute tests or lints, check these files.

## Testing

Refer to the "Running Go tests" and "Running tests with Fivetran SDK tester" sections in [CONTRIBUTING.md](./CONTRIBUTING.md) for full details.

**Test file naming conventions:**

- `*_test.go` — unit tests (no external dependencies)
- `*_integration_test.go` — require a running ClickHouse instance (`docker compose up -d`)
- `*_e2e_test.go` — end-to-end tests that start the gRPC server and use the Fivetran SDK tester

All tests run with `make test`. Integration and e2e tests need ClickHouse running locally via Docker first.

**SDK test inputs** are JSON files in `sdk_tests/` (e.g., `input_all_data_types.json`). Configuration for the SDK tester is in `sdk_tests/configuration.json` (copied from `sdk_tests/default_configuration.json` if missing).

**Schema migration test inputs** are also in `sdk_tests/`:
- `schema_migrations_input_ddl.json` — tests DDL operations (add_column, change_column_data_type, drop_column) which go through the `AlterTable` RPC
- `schema_migrations_input_dml.json` — tests DML operations (copy_column, update_column_value, add_column_with_default_value, set_column_to_null, copy_table, rename_column, rename_table, drop_table) which go through the `Migrate` RPC
- `schema_migrations_input_sync_modes.json` — tests history mode and sync mode operations (add_column_in_history_mode, drop_column_in_history_mode, copy_table_to_history_mode, migrate_soft_delete_to_history, migrate_history_to_soft_delete) which go through the `Migrate` RPC

**Writing tests:** keep tests concise and table-driven where possible. Reference these files as style templates:

- Unit tests: [destination/db/sql/sql_test.go](./destination/db/sql/sql_test.go)
- Integration tests: [destination/db/clickhouse_integration_test.go](./destination/db/clickhouse_integration_test.go)
- E2E tests: [destination/main_e2e_test.go](./destination/main_e2e_test.go)

## Gotchas

- Never edit files in `proto/` — they are generated. Run `make generate-proto` instead.
- Tests require `127.0.0.1 clickhouse` in `/etc/hosts` (see [CONTRIBUTING.md](./CONTRIBUTING.md)).
- Integration and e2e tests require ClickHouse running via `docker compose up -d`. Sometimes the container may be already running, so you can check with `docker ps` and skip the step if you are sure it is running.
- `sdk_tests/configuration.json` is gitignored. It is auto-created from `sdk_tests/default_configuration.json` when running `make test`.
- Flags are defined as package-level vars in [destination/common/flags/flags.go](./destination/common/flags/flags.go). Some flags can be overridden via the `advanced_config` field (see CONTRIBUTING.md for details).

### Schema migration gotchas

- **SDK tester `schema_migration` operation mapping**: DDL operations (`add_column`, `change_column_data_type`, `drop_column`) in the test JSON are sent as `AlterTable` RPC calls. All other operations (`copy_column`, `rename_column`, `update_column_value`, `copy_table`, etc.) are sent as `Migrate` RPC calls.
- **`set_column_to_null`**: The SDK tester sends this as an `UpdateColumnValueOperation` with `value = "NULL"` (the literal string), not an empty string. The Migrate handler must detect both `""` and `"NULL"` as null indicators.
- **`drop_column_in_history_mode`**: The SDK tester validates that the column is physically removed from the table after the operation. The implementation must insert new history versions, close old active rows, AND then `ALTER TABLE DROP COLUMN`.
- **ClickHouse column ordering**: New columns added via `ALTER TABLE ADD COLUMN` appear at the end of the table, after existing columns like `_fivetran_synced`. Test assertions must match this physical column order, not a logical/expected order.
- **`copy_table_to_history_mode` / `soft_delete_to_history`**: The source table may not have a `_fivetran_deleted` column (e.g., tables created with `history_mode: false` in the SDK tester). The implementation must check if the soft-delete column exists before referencing it in SQL. If absent, treat all rows as active.
- **History mode add/drop column ordering**: INSERT new active rows first, then UPDATE to close old active rows. The reverse order would cause the UPDATE to read stale data since `mutations_sync=3` makes operations synchronous.
- **Sync mode transitions (full table rebuild)**: Adding/removing `_fivetran_start` from ORDER BY requires creating a new table, INSERT...SELECT, and rename swap — same pattern as `AlterTable` with PK changes. Use two separate RENAME statements (replicated DB limitation).
- **LIVE mode transitions**: `SOFT_DELETE_TO_LIVE`, `HISTORY_TO_LIVE`, `LIVE_TO_SOFT_DELETE`, `LIVE_TO_HISTORY` are not available in the current Partner SDK version. Return `unsupported` for these.

## Debugging

To debug SDK test scenarios without modifying Go test code, run the destination server and SDK tester separately. See the "Running tests with Fivetran SDK tester" section in [CONTRIBUTING.md](./CONTRIBUTING.md).

## Code style

- Linter: [golangci-lint](https://golangci-lint.run) configured in [.golangci.yml](./.golangci.yml). Run with `make lint`.
- Use `fmt.Errorf` with `%w` for error wrapping.
- Logging uses `zerolog` via the `destination/common/log` package — use `log.Info`, `log.Warn`, `log.Error`.
- Assertions in tests use `github.com/stretchr/testify` (`assert` and `require`).
- Avoid adding package-level comments (suppressed in linter config via `ST1000`).

## PR review

PR template is in [.github/pull_request_template.md](./.github/pull_request_template.md).

**Review priorities** (in order): correctness, data integrity, backwards compatibility, performance, maintainability, test coverage.

When reviewing PRs, structure the review into these sections:

1. **Correctness & Data Integrity** — Does the logic handle edge cases? Could data be silently lost or corrupted?
2. **ClickHouse-specific concerns** — Mutations are heavyweight; prefer metadata-only operations (RENAME COLUMN, ADD COLUMN) where possible. Verify ORDER BY / ReplacingMergeTree implications.
3. **Error handling & observability** — Are errors wrapped with `%w`? Are failures logged with enough context?
4. **Tests** — Are new/changed code paths covered? Do tests follow the table-driven style?

Classify each issue by severity:
- **Must fix** — Bug, data loss risk, or broken contract. Blocks merge.
- **Should fix** — Meaningful improvement (perf, readability, edge case). Strongly recommended before merge.
- **Nit** — Style, naming, minor suggestion. Non-blocking.

For each issue, provide: file and line reference, rationale, and a suggested fix.

**Checklist** (answer yes/no):
- Backwards-compatible change?
- Existing tests still pass?
- New tests added for new behavior?
- Documentation updated if user-facing behavior changed? (see [docs/overview.md](./docs/overview.md) and [docs/schema_operations.md](./docs/schema_operations.md))

## Documentation

User-facing documentation lives in `docs/`. When changing behavior that affects data type mappings, table structure, sync modes, or migration operations, update the relevant docs. The PR template includes a checklist item for this.

## MCP servers

- **clickhouse-docs** — search ClickHouse documentation for SQL syntax, data types, engine behavior, and configuration. Use when you need to verify ClickHouse-specific behavior rather than guessing.
