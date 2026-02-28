# Mutation Batch Size Finder

Script to determine the optimal `MutationBatchSize` and `HardDeleteBatchSize` values for a given ClickHouse instance with standard `max_ast_elements` and `max_query_size` settings.

You can modify the table definition in both tests to test different scenarios.

## Prerequisites

A running ClickHouse instance, e.g. start one locally with Docker:

```sh
docker compose up -d
```

## Usage

From the repository root:

```sh
# Run both test scenarios (simple + realistic schemas)
go test -v ./internal/scripts/mutation_batch_size/

# Run only the simple schema test
go test -v -run TestFindOptimalMutationBatchSize ./internal/scripts/mutation_batch_size/

# Run only the realistic schema test (4 PKs with large integers)
go test -v -run TestFindRealisticMutationBatchSize ./internal/scripts/mutation_batch_size/
```

The output will show the maximum working batch size for each mutation type and recommend
safe values at 80% of the limit.
