_:
	@echo -e "Check Makefile for all available targets"

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
ifeq ($(UNAME_S),Darwin)
    GRPC_HOSTNAME := host.docker.internal
    PROTOC_OS := osx
else
    GRPC_HOSTNAME := 172.17.0.1
    PROTOC_OS := linux
endif
ifneq ($(filter $(UNAME_M),arm64 aarch64),)
    PROTOC_ARCH := aarch_64
else
    PROTOC_ARCH := x86_64
endif

FIVETRAN_TAG     = "8b30d60b8eb2040f858c3f3c1ab819daed9fd84d"
FIVETRAN_SDK_URL = "https://raw.githubusercontent.com/fivetran/fivetran_partner_sdk/$(FIVETRAN_TAG)"

SDK_TESTER_VERSION = "2.26.0408.001"
SDK_TESTER_IMAGE   = "us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:$(SDK_TESTER_VERSION)"

PROTOC_VERSION = "31.1"
PROTOC_GEN_GO_VERSION = "v1.36.10"
PROTOC_GEN_GO_GRPC_VERSION = "v1.5.1"
GOLANG_CI_LINT_VERSION = $(shell cat .golangci-lint-version)

GOPATH_BIN = $(shell go env GOPATH)/bin

# Optional: set GITHUB_TOKEN to authenticate GitHub downloads and avoid
# rate limiting (HTTP 429) on raw.githubusercontent.com.
ifneq ($(strip $(GITHUB_TOKEN)),)
    GITHUB_AUTH = -H "Authorization: Bearer $(GITHUB_TOKEN)"
endif

prepare-fivetran-sdk:
	mkdir -p proto
	curl -fL --retry 3 $(GITHUB_AUTH) -o proto/common.proto          "$(FIVETRAN_SDK_URL)/common.proto"
	curl -fL --retry 3 $(GITHUB_AUTH) -o proto/destination_sdk.proto "$(FIVETRAN_SDK_URL)/destination_sdk.proto"

# Installs a pinned protoc version into .protoc/ so builds do not depend on
# whatever protoc happens to be on PATH.
install-protoc:
	rm -rf .protoc
	mkdir -p .protoc
	curl -fL --retry 3 -o .protoc/protoc.zip \
		"https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(PROTOC_OS)-$(PROTOC_ARCH).zip"
	unzip -o .protoc/protoc.zip -d .protoc
	rm .protoc/protoc.zip

# GOOS/GOARCH are unset here so the protoc plugins are always built for the
# build host, even when cross-compiling the destination binary itself.
install-protoc-gen-go:
	env -u GOOS -u GOARCH go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	env -u GOOS -u GOARCH go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

generate-proto:
	rm -f proto/*.go
	PATH="$$PWD/.protoc/bin:$(GOPATH_BIN):$$PATH" protoc \
        --proto_path=proto \
        --go_out=proto \
        --go_opt=paths=source_relative \
        --go-grpc_out=proto \
        --go-grpc_opt=paths=source_relative \
        common.proto \
        destination_sdk.proto

pull-sdk-tester:
	docker pull $(SDK_TESTER_IMAGE)

sdk-test:
	docker run --mount type=bind,source=$$PWD/sdk_tests,target=/data \
		-a STDIN -a STDOUT -a STDERR \
		-e WORKING_DIR=$$PWD/sdk_tests \
		-e GRPC_HOSTNAME=$(GRPC_HOSTNAME) \
		--network=host \
		$(SDK_TESTER_IMAGE) \
		--tester-type destination --port 50052 $$TEST_ARGS

recreate-test-db:
	curl --data-binary "DROP DATABASE IF EXISTS tester" http://localhost:8123
	curl --data-binary "CREATE DATABASE tester" http://localhost:8123

lint:
	docker run --rm -v $$PWD:/destination -v golangci-lint-cache:/root/.cache -w /destination golangci/golangci-lint:$(GOLANG_CI_LINT_VERSION) golangci-lint run -v

test:
	test -f sdk_tests/configuration.json || cp sdk_tests/default_configuration.json sdk_tests/configuration.json
	go test fivetran.com/fivetran_sdk/destination/... -count=1 -v -race $$TEST_ARGS

test-with-coverage:
	TEST_ARGS="-coverpkg=fivetran.com/fivetran_sdk/destination/... -coverprofile cover.out" make test
	go tool cover -func=cover.out

# Compile only — assumes protos are already generated and tools installed.
build-server:
	rm -rf ./bin
	go build -o ./bin/server ./destination
	chmod a+x ./bin/server

# Fivetran binary build pipeline requirements:
# - downloads protos
# - installs protoc and its Go plugins
# - generates code
# - produces the binary at ./bin/server
build: prepare-fivetran-sdk install-protoc install-protoc-gen-go generate-proto build-server

build-docker-ci:
	docker compose -f docker-compose.ci.yml build destination --no-cache

dependency-graph:
	godepgraph -p github.com,google,golang -s fivetran.com/fivetran_sdk/destination | dot -Tpng -o godepgraph.png
	xdg-open godepgraph.png || open godepgraph.png

run:
	make build-server
	./bin/server

.PHONY: _ prepare-fivetran-sdk install-protoc install-protoc-gen-go generate-proto start-docker run lint test go-test go-test-with-coverage build build-server clickhouse-query-for-tests build-docker-ci pull-sdk-tester
