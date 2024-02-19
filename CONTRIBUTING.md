## Pre-requisites

* Git
* Make (see [Makefile](./Makefile) for all available commands)
* Go 1.20+

## Install Protoc and Go plugin

Download the protocol buffer compiler suitable for your platform from
the [releases page](https://github.com/protocolbuffers/protobuf/releases/tag/v25.2) and install it to `$HOME/.local`.

For example (Linux x86_64):

```bash
unzip protoc-25.2-linux-x86_64.zip -d $HOME/.local
```

Then, install the Go plugin for the protocol buffer compiler:

```bash
make install-protoc-gen-go
```

## Generate Go code from the Protobuf definitions

Clone the Fivetran SDK repo first. It contains
the [protobuf definitions](https://github.com/fivetran/fivetran_sdk/blob/main/destination_sdk.proto) for the destination
GRPC server.

```bash
make prepare-fivetran-sdk
```

Then, generate the Go code:

```bash
make generate-proto
```

## Running Go tests

Start ClickHouse server in Docker:

```bash
docker compose up -d
```

Test files marked as `_integration_test` require ClickHouse connection.

Test files marked as `_e2e_test` (like [destination/main_e2e_test.go](./destination/main_e2e_test.go))
require ClickHouse connection, and will also start the destination GRPC server.

Run the tests:

```bash
make test
```

## Running tests with Fivetran SDK tester

Fivetran SDK tests are part of the normal Go test run,
see [destination/main_e2e_test.go](./destination/main_e2e_test.go);
however, it is possible to execute them in a stand-alone mode, which might be useful for debugging purposes,
if you don't want to modify the test code in Go.

Start ClickHouse server in Docker:

```bash
docker compose up -d
```

Start the destination app:

```bash
make run
```

Run the SDK tester with a particular input file from [sdk_tests](./sdk_tests) directory,
for example, `input_all_data_types.json`:

```bash
TEST_ARGS=--input-file=input_all_data_types.json make sdk-test
```

Run the SDK tester with all input JSON files (see [sdk_tests](./sdk_tests) directory):

```bash
make sdk-test
```

See also: Fivetran SDK
tester [documentation](https://github.com/fivetran/fivetran_sdk/tree/main/tools/destination-tester).

## Lint

Runs [golangci-lint](https://golangci-lint.run) in Docker to lint the Go code:

```bash
make lint
```

## Building a Docker image

```bash
make build
docker build . -t clickhouse-fivetran-destination
```

You should be able to run the destination app:

```bash
docker run clickhouse-fivetran-destination
```

## Available flags

List of available flags for the destination app:

```sh
make build
./out/clickhouse_destination -h
```

Check the [main.go](./destination/main.go) file for more details.
