_:
	@echo -e "Check Makefile for all available targets"

prepare-fivetran-sdk:
	rm -rf fivetran_sdk
	git clone --depth 1 https://github.com/fivetran/fivetran_sdk.git fivetran_sdk
	mkdir -p proto

install-protoc-gen-go:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

generate-proto:
	rm -f proto/*.proto
	rm -f proto/*.go
	cp fivetran_sdk/common.proto fivetran_sdk/destination_sdk.proto proto/
	protoc \
        --proto_path=proto \
        --go_out=proto \
        --go_opt=paths=source_relative \
        --go-grpc_out=proto \
        --go-grpc_opt=paths=source_relative \
        common.proto \
        destination_sdk.proto

sdk-test:
	docker run --mount type=bind,source=$$PWD/sdk_tests,target=/data \
		-a STDIN -a STDOUT -a STDERR \
		-e WORKING_DIR=$$PWD/sdk_tests \
		-e GRPC_HOSTNAME=172.17.0.1 \
		--network=host \
		fivetrandocker/sdk-destination-tester:024.0314.001 $$TEST_ARGS

recreate-test-db:
	curl --data-binary "DROP DATABASE IF EXISTS tester" http://localhost:8123
	curl --data-binary "CREATE DATABASE tester" http://localhost:8123

lint:
	docker run --rm -v $$PWD:/destination -w /destination golangci/golangci-lint:v1.55.2 golangci-lint run -v

test:
	test -f sdk_tests/configuration.json || cp sdk_tests/default_configuration.json sdk_tests/configuration.json
	go test fivetran.com/fivetran_sdk/destination/... -count=1 -v -race $$TEST_ARGS

test-with-coverage:
	TEST_ARGS="-coverpkg=fivetran.com/fivetran_sdk/destination/... -coverprofile cover.out" make test
	go tool cover -func=cover.out

build:
	rm -rf ./out
	go build -o ./out/clickhouse_destination ./destination
	chmod a+x ./out/clickhouse_destination

build-docker-ci:
	docker compose -f docker-compose.ci.yml build destination --no-cache

dependency-graph:
	godepgraph -p github.com,google,golang -s fivetran.com/fivetran_sdk/destination | dot -Tpng -o godepgraph.png
	xdg-open godepgraph.png || open godepgraph.png

run:
	make build
	./out/clickhouse_destination

.PHONY: _ prepare-fivetran-sdk generate-proto start-docker run lint test go-test go-test-with-coverage build clickhouse-query-for-tests build-docker-ci
