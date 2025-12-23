## Pre-requisites

* Git
* Make (see [Makefile](./Makefile) for all available commands)
* Go 1.22+

## Install Protoc and Go plugin

### Mac OS

Install via HomeBrew:

```bash
brew install protobuf
```

### Linux

Download the protocol buffer compiler suitable for your platform from
the [releases page](https://github.com/protocolbuffers/protobuf/releases/latest) and install it to `$HOME/.local`.

For example (Linux x86_64):

```bash
unzip protoc-*.zip -d $HOME/.local
```

Make sure that you have 

```bash
# Golang, protoc-gen-go, gopls
export PATH="$HOME/.local/bin:$HOME/go/bin:$PATH"
```

in your `.bashrc`/`.zshrc`.

(macOS only): try to run `protoc` from the terminal. If `protoc` cannot be executed due to the OS security policy, 
go to Settings -> Privacy & Security, find "protoc" there, and click "Allow anyway". 
Run `protoc` one more time from the terminal, and click "Open".

Then, install the Go plugin for the protocol buffer compiler:

```bash
make install-protoc-gen-go
```

## Generate Go code from the Protobuf definitions

Download the Fivetran SDK protobuf files first. These files contain
the [types definitions](https://github.com/fivetran/fivetran_sdk/blob/main/destination_sdk.proto) for the destination
GRPC server. 

You can do it by executing the following command:

```bash
make prepare-fivetran-sdk
```

Then, generate the Go code:

```bash
make generate-proto
```

Verify that the application can be built:

```bash
make build
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
make recreate-test-db && TEST_ARGS=--input-file=input_all_data_types.json make sdk-test
```

Run the SDK tester with all input JSON files (see [sdk_tests](./sdk_tests) directory):

```bash
make recreate-test-db && make sdk-test
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

Check the [flags.go](./destination/common/flags/flags.go) file for more details.
