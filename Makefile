_:
	@echo -e "Check Makefile for all available targets"

prepare-fivetran-sdk:
	rm -rf fivetran_sdk
	git clone --depth 1 https://github.com/fivetran/fivetran_sdk.git fivetran_sdk
	mkdir -p proto

start-docker:
	docker-compose up -d

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

test:
	curl --data-binary "DROP DATABASE IF EXISTS tester" http://localhost:8123
	curl --data-binary "CREATE DATABASE tester" http://localhost:8123
	docker run --mount type=bind,source=$$PWD/sdk_tests,target=/data \
		-a STDIN -a STDOUT -a STDERR -it \
		-e WORKING_DIR=$$PWD/sdk_tests \
		-e GRPC_HOSTNAME=host.docker.internal \
		--network=host \
		it5t/fivetran-sdk-destination-tester:024.0116.001

go-test:
	go test fivetran.com/fivetran_sdk/destination -count=1

run:
	go run destination/main.go

.PHONY: _ prepare-fivetran-sdk generate-proto start-docker run test go-test
