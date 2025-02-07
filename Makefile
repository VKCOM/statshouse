BUILD_VERSION   := $(if $(BUILD_VERSION),$(BUILD_VERSION),$(shell git describe --tags --always --dirty))
BUILD_COMMIT    := $(if $(BUILD_COMMIT),$(BUILD_COMMIT),$(shell git log --format="%H" -n 1))
BUILD_COMMIT_TS := $(if $(BUILD_COMMIT_TS),$(BUILD_COMMIT_TS),$(shell git log --format="%ct" -n 1))
BUILD_TIME      := $(if $(BUILD_TIME),$(BUILD_TIME),$(shell date +%FT%T%z))
BUILD_MACHINE   := $(if $(BUILD_MACHINE),$(BUILD_MACHINE),$(shell uname -n -m -r -s))
REACT_APP_BUILD_VERSION := $(if $(REACT_APP_BUILD_VERSION),$(REACT_APP_BUILD_VERSION),$(BUILD_VERSION)-$(BUILD_TIME))
TL_BYTE_VERSIONS := statshouse.
# TODO: BUILD_ID

COMMON_BUILD_VARS := -X 'github.com/vkcom/statshouse/internal/vkgo/build.time=$(BUILD_TIME)' \
	-X 'github.com/vkcom/statshouse/internal/vkgo/build.machine=$(BUILD_MACHINE)' \
	-X 'github.com/vkcom/statshouse/internal/vkgo/build.commit=$(BUILD_COMMIT)' \
	-X 'github.com/vkcom/statshouse/internal/vkgo/build.version=$(BUILD_VERSION)' \
	-X 'github.com/vkcom/statshouse/internal/vkgo/build.number=$(BUILD_ID)' \
	-X 'github.com/vkcom/statshouse/internal/vkgo/build.commitTimestamp=$(BUILD_COMMIT_TS)' \
	-X 'github.com/vkcom/statshouse/internal/vkgo/build.trustedSubnetGroups=$(BUILD_TRUSTED_SUBNET_GROUPS)'

COMMON_LDFLAGS = $(COMMON_BUILD_VARS) -extldflags '-O2'

.PHONY: all build-go build-ui \
	build-sh build-sh-api build-sh-api-embed build-sh-metadata build-sh-grafana \
	build-sh-ui build-grafana-ui

all: build-go build-ui
build-go: build-sh build-sh-api build-sh-metadata build-sh-grafana build-igp build-agg
build-ui: build-sh-ui build-grafana-ui
build-main-daemons: build-sh build-sh-api build-sh-metadata build-igp build-agg

build-sh:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse ./cmd/statshouse

build-sh-api:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-api ./cmd/statshouse-api

build-sh-api-embed:
	go build -tags embed -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-api ./cmd/statshouse-api

build-sh-metadata:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-metadata ./cmd/statshouse-metadata

build-igp:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-igp ./cmd/statshouse-igp

build-agg:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-agg ./cmd/statshouse-agg

build-sh-grafana:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-grafana-plugin ./cmd/statshouse-grafana-plugin

build-sh-ui:
	cd statshouse-ui && npm clean-install && NODE_ENV=production REACT_APP_BUILD_VERSION=$(REACT_APP_BUILD_VERSION) npm run build
	rm -rf cmd/statshouse-api/build || true
	cp -r statshouse-ui/build cmd/statshouse-api/ || true

build-grafana-ui:
	cd grafana-plugin-ui && npm clean-install && npm run build

build-deb:
	./build/makedeb.sh

.PHONY: gen gen-sqlite gen-easyjson-format gen-easyjson-handler
gen:
	go run github.com/vkcom/tl/cmd/tlgen@v1.1.13 --language=go --outdir=./internal/data_model/gen2 -v \
		--generateRPCCode=true \
		--pkgPath=github.com/vkcom/statshouse/internal/data_model/gen2/tl \
 		--basicPkgPath=github.com/vkcom/statshouse/internal/vkgo/basictl \
 		--basicRPCPath=github.com/vkcom/statshouse/internal/vkgo/rpc \
 		--generateByteVersions=$(TL_BYTE_VERSIONS) \
 		--copyrightPath=./copyright \
		./internal/data_model/api.tl \
		./internal/data_model/common.tl \
		./internal/data_model/engine.tl \
		./internal/data_model/metadata.tl \
		./internal/data_model/public.tl \
		./internal/data_model/schema.tl
	@echo "Checking that generated code actually compiles..."
	@go build ./internal/data_model/gen2/...

gen-sqlite:
	go run github.com/vkcom/tl/cmd/tlgen@v1.1.13 --language=go --outdir=./internal/sqlitev2/checkpoint/gen2 -v \
		--generateRPCCode=true \
		--pkgPath=github.com/vkcom/statshouse/internal/sqlitev2/checkpoint/gen2/tl \
 		--basicPkgPath=github.com/vkcom/statshouse/internal/vkgo/basictl \
 		--basicRPCPath=github.com/vkcom/statshouse/internal/vkgo/rpc \
 		--generateByteVersions=sqlite. \
 		--copyrightPath=./copyright \
		./internal/data_model/common.tl \
		./internal/sqlitev2/checkpoint/metainfo.tl
	@echo "Checking that generated code actually compiles..."
	@go build ./internal/sqlitev2/checkpoint/gen2/...

gen-easyjson-format:
	go generate ./internal/format/format.go

gen-easyjson-handler:
	go generate ./internal/api/handler.go
	@echo "!!!You need to manually updater handler_easyjson.go to convert NaN to null!!!"

.PHONY: lint test check
lint:
	staticcheck -version
	staticcheck ./...

test:
	go test -v -race ./...

check: lint test
