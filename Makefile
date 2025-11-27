_:
	@echo -e "Check Makefile for all available targets"

fivetran_tag     = "8b30d60b8eb2040f858c3f3c1ab819daed9fd84d"
fivetran_sdk_url = "https://raw.githubusercontent.com/fivetran/fivetran_partner_sdk/$(fivetran_tag)"

prepare-fivetran-sdk:
	mkdir -p proto
	curl -o proto/common.proto          "$(fivetran_sdk_url)/common.proto"
	curl -o proto/destination_sdk.proto "$(fivetran_sdk_url)/destination_sdk.proto"

install-protoc-gen-go:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

generate-proto:
	rm -f proto/*.go
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
		us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:2.25.1118.001 \
		--tester-type destination --port 50052 $$TEST_ARGS

recreate-test-db:
	curl --data-binary "DROP DATABASE IF EXISTS tester" http://localhost:8123
	curl --data-binary "CREATE DATABASE tester" http://localhost:8123

lint:
	docker run --rm -v $$PWD:/destination -w /destination golangci/golangci-lint:v2.6.2 golangci-lint run -v

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
