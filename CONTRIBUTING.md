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

Start the ClickHouse server in Docker first, as some of the tests require a database connection:

```bash
docker-compose up -d
```

Run the tests:

```bash
make go-tests
```

## Running tests with Fivetran SDK tester

Having ClickHouse server started, run the destination app:

```bash
make start
```

Run the tests (all input files, see [sdk_tests](./sdk_tests) directory):

```bash
make test
```

Run a particular input file, for example, `input_3.json`:

```bash
TEST_ARGS=--input-file=input_3.json make test
```

See Fivetran SDK tester [documentation](https://github.com/fivetran/fivetran_sdk/tree/main/tools/destination-tester) for
more details.

## Lint

Runs [golangci-lint](https://golangci-lint.run) in Docker to lint the Go code:

```bash
make lint
```

## Building a Docker image

```bash
make compile
docker build . -t clickhouse-fivetran-destination
```

You should be able to run the destination app:

```bash
docker run clickhouse-fivetran-destination
```

## Available flags

```
  -delete-batch-size uint
        Batch size for WriteBatch/Delete operations (default 1000)
  -dev boolean
        Whether the server is running in development mode, mainly for pretty logging (default false)
  -max-idle-connections uint
        Max number of idle connections for ClickHouse client (default 5)
  -max-open-connections uint
        Max number of open connections for ClickHouse client (recommended: max-idle-connections + 5) (default 10)
  -max-parallel-updates uint
        Max number of parallel batches to insert for WriteBatch/Update or WriteBatch/Delete operations (default 5)
  -max-retries uint
        Max number of retries for ClickHouse client in case of network errors (default 30)
  -port uint
        Listen port (default 50052)
  -replace-batch-size uint
        Batch size for WriteBatch/Replace operations (default 100000)
  -retry-delay-ms uint
        Delay in milliseconds for retries in case of network errors (default 1000)
  -update-batch-size uint
        Batch size for WriteBatch/Update operations (default 1000)
```