## Pre-requisites

* Git
* Make (see [Makefile](./Makefile) for all available commands)
* Go 1.22+

## Building

The `build` target is fully self-contained (as required by the
[Fivetran binary build pipeline](https://github.com/fivetran/fivetran_partner_sdk/blob/main/development-guide/binary-build-requirements.md)):
it downloads the Fivetran SDK protobuf files, installs a pinned version of `protoc` into the
local `.protoc/` directory, installs the Go protoc plugins, generates the Go code, and
builds the binary at `./bin/server`:

```bash
make build
```

No manual `protoc` installation is required.

Tip: if a GitHub token is available, export it as `GITHUB_TOKEN` to authenticate the proto
downloads and avoid GitHub rate limiting (HTTP 429), e.g. `GITHUB_TOKEN=$(gh auth token) make build`.

Once the protos have been generated, you can rebuild just the server binary (much faster,
no downloads):

```bash
make build-server
```

The individual steps are also available as separate targets, e.g. if you only need to
re-download the protos or regenerate the Go code:

```bash
make prepare-fivetran-sdk   # download proto files from the Fivetran SDK repo
make install-protoc         # install pinned protoc into .protoc/
make install-protoc-gen-go  # install the Go protoc plugins
make generate-proto         # generate Go code from the proto files
```

## Running Go tests

Add ClickHouse to your `/etc/hosts` file:

```bash
echo "127.0.0.1 clickhouse" | sudo tee -a /etc/hosts
```

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

Pull the SDK tester image:

```bash
make pull-sdk-tester
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

After running, you can inspect the resulting tables directly:

```bash
# Check table schema
curl --data-binary "SELECT name, type, comment FROM system.columns WHERE database = 'tester' AND table = 'transaction' FORMAT CSV" http://localhost:8123

# Check table data
curl --data-binary "SELECT * FROM tester.transaction FINAL FORMAT CSV" http://localhost:8123
```

See also: Fivetran SDK
tester [documentation](https://github.com/fivetran/fivetran_sdk/tree/main/tools/destination-tester)
and the [Schema Migration Helper Guide](https://github.com/fivetran/fivetran_partner_sdk/blob/f61fa5186477d25fb3564233773e0123e73c29bd/schema-migration-helper-service.md) for the `Migrate` RPC contract.

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
make build-server
./bin/server -h
```

Check the [flags.go](./destination/common/flags/flags.go) file for more details.

## testing the advanced configuration

You can test the advanced configuration by adding the json file encoded as base64 to the `advanced_config` field in the [configuration.json](./sdk_tests/configuration.json) file.

For example, given the following JSON file:
```json
{
  "destination_configurations": {
    "write_batch_size": 50000
  }
}
```

Base64-encode it:
```bash
echo -n '{"destination_configurations":{"write_batch_size":50000}}' | base64
```
Add it to sdk_tests/configuration.json:
```json
{  
    "host": "clickhouse",
    "port": "9000",
    "password": "",
    "username": "default",
    "local": "true",
    "advanced_config": "eyJkZXN0aW5hdGlvbl9jb25maWd1cmF0aW9ucyI6eyJ3cml0ZV9iYXRjaF9zaXplIjo1MDAwMH19"
}
```
And then execute the tests:
```bash

# Terminal 1: ClickHouse
docker compose up -d

# Terminal 2: destination service
make run

# Terminal 3: SDK tester
make recreate-test-db
make sdk-test
```
The destination service will log all flag values at startup
