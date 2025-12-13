BUILD_VERSION   := $(if $(BUILD_VERSION),$(BUILD_VERSION),$(shell git describe --tags --always --dirty))
BUILD_COMMIT    := $(if $(BUILD_COMMIT),$(BUILD_COMMIT),$(shell git log --format="%H" -n 1))
BUILD_COMMIT_TS := $(if $(BUILD_COMMIT_TS),$(BUILD_COMMIT_TS),$(shell git log --format="%ct" -n 1))
BUILD_TIME      := $(if $(BUILD_TIME),$(BUILD_TIME),$(shell date +%FT%T%z))
BUILD_MACHINE   := $(if $(BUILD_MACHINE),$(BUILD_MACHINE),$(shell uname -n -m -r -s))
REACT_APP_BUILD_VERSION := $(if $(REACT_APP_BUILD_VERSION),$(REACT_APP_BUILD_VERSION),$(BUILD_VERSION)-$(BUILD_TIME))
TL_BYTE_VERSIONS := statshouse.
# TODO: BUILD_ID

COMMON_BUILD_VARS := -X 'github.com/VKCOM/statshouse/internal/vkgo/build.time=$(BUILD_TIME)' \
	-X 'github.com/VKCOM/statshouse/internal/vkgo/build.machine=$(BUILD_MACHINE)' \
	-X 'github.com/VKCOM/statshouse/internal/vkgo/build.commit=$(BUILD_COMMIT)' \
	-X 'github.com/VKCOM/statshouse/internal/vkgo/build.version=$(BUILD_VERSION)' \
	-X 'github.com/VKCOM/statshouse/internal/vkgo/build.number=$(BUILD_ID)' \
	-X 'github.com/VKCOM/statshouse/internal/vkgo/build.commitTimestamp=$(BUILD_COMMIT_TS)' \
	-X 'github.com/VKCOM/statshouse/internal/vkgo/build.trustedSubnetGroups=$(BUILD_TRUSTED_SUBNET_GROUPS)'

COMMON_LDFLAGS = $(COMMON_BUILD_VARS) -extldflags '-O2'

.PHONY: all build-go build-ui \
	build-sh build-sh-api build-sh-api-noembed build-sh-metadata build-sh-grafana \
	build-sh-ui build-grafana-ui

all: build-ui build-go # order important
build-go: build-sh build-sh-api build-sh-metadata build-sh-grafana build-igp build-agg
build-ui: build-sh-ui build-grafana-ui
build-main-daemons: build-sh build-sh-api build-sh-metadata build-igp build-agg

# to build tools for running on adm512, disable cgo
# CGO_ENABLED=0 go build ...
build-sh:
	CGO_ENABLED=0 go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse ./cmd/statshouse

build-sh-api:
	rm -rf cmd/statshouse-api/build || true # remove old dir from previous builds
	CGO_ENABLED=0 go build -tags embed -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-api ./cmd/statshouse-api

build-sh-api-noembed:
	rm -rf cmd/statshouse-api/build || true # remove old dir from previous builds
	CGO_ENABLED=0 go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-api ./cmd/statshouse-api

build-sh-metadata:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-metadata ./cmd/statshouse-metadata

build-igp:
	CGO_ENABLED=0 go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-igp ./cmd/statshouse-igp

build-agg:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-agg ./cmd/statshouse-agg

build-sh-grafana:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-grafana-plugin ./cmd/statshouse-grafana-plugin

build-sh-ui:
	cd statshouse-ui && npm clean-install && NODE_ENV=production REACT_APP_BUILD_VERSION=$(REACT_APP_BUILD_VERSION) npm run build

build-grafana-ui:
	cd grafana-plugin-ui && npm clean-install && npm run build

build-deb:
	./build/makedeb.sh

.PHONY: gen
gen: gen-tl gen-sqlite gen-easyjson gen-yaml

gen-tl: ./internal/data_model/api.tl ./internal/data_model/common.tl ./internal/data_model/engine.tl ./internal/data_model/metadata.tl ./internal/data_model/public.tl ./internal/data_model/schema.tl
	go run github.com/vkcom/tl/cmd/tlgen@v1.2.25 --language=go --outdir=./internal/data_model/gen2 -v \
		--generateRPCCode=true \
		--pkgPath=github.com/VKCOM/statshouse/internal/data_model/gen2/tl \
		--basicPkgPath=github.com/VKCOM/statshouse/internal/vkgo/basictl \
		--basicRPCPath=github.com/VKCOM/statshouse/internal/vkgo/rpc \
		--generateByteVersions=$(TL_BYTE_VERSIONS) \
		--rawHandlerWhiteList=statshouse.,metadata. \
		--copyrightPath=./copyright \
		./internal/data_model/api.tl \
		./internal/data_model/common.tl \
		./internal/data_model/engine.tl \
		./internal/data_model/metadata.tl \
		./internal/data_model/public.tl \
		./internal/data_model/schema.tl
	@echo "Checking that generated code actually compiles..."
	@go build ./internal/data_model/gen2/...

gen-sqlite: ./internal/data_model/common.tl ./internal/sqlitev2/checkpoint/metainfo.tl
	go run github.com/vkcom/tl/cmd/tlgen@v1.2.19 --language=go --outdir=./internal/sqlitev2/checkpoint/gen2 -v \
		--generateRPCCode=true \
		--pkgPath=github.com/VKCOM/statshouse/internal/sqlitev2/checkpoint/gen2/tl \
		--basicPkgPath=github.com/VKCOM/statshouse/internal/vkgo/basictl \
		--basicRPCPath=github.com/VKCOM/statshouse/internal/vkgo/rpc \
		--generateByteVersions=sqlite. \
		--copyrightPath=./copyright \
		./internal/data_model/common.tl \
		./internal/sqlitev2/checkpoint/metainfo.tl
	@echo "Checking that generated code actually compiles..."
	@go build ./internal/sqlitev2/checkpoint/gen2/...

gen-easyjson: ./internal/format/format.go ./internal/api/handler.go ./internal/api/httputil.go
	@echo "you may need to install easyjson version: go install github.com/mailru/easyjson/...@latest"
	go generate ./internal/api/handler.go
	go generate ./internal/format/format.go

gen-yaml: ./internal/promql/parser/parse.y
	@echo "you may need to install yaml version: go install gopkg.in/yaml.v2/...@latest"
	go generate ./internal/format/format.go

.PHONY: lint test check
lint:
	go run honnef.co/go/tools/cmd/staticcheck@latest -version
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...

test:
	CGO_LDFLAGS="-w" go test -race ./...

# only tests that run local ClickHouse
test-integration:
	CGO_LDFLAGS="-w" go test -v -race -tags integration -run '.*Integration'  ./...

check: lint test

upgrade_mod:
	# commented those, which need code regeneration
	go get -u github.com/ClickHouse/ch-go
	go get -u github.com/ClickHouse/clickhouse-go/v2
	go get -u github.com/VKCOM/statshouse-go
	go get -u github.com/cloudflare/tableflip
	go get -u github.com/dchest/siphash
	go get -u github.com/dgryski/go-maglev
	go get -u github.com/fsnotify/fsnotify
	go get -u github.com/go-kit/log
	# github.com/gogo/protobuf
	go get -u github.com/golang-jwt/jwt/v4
	go get -u github.com/google/btree
	go get -u github.com/google/go-cmp
	go get -u github.com/google/uuid
	go get -u github.com/gorilla/handlers
	go get -u github.com/gorilla/mux
	go get -u github.com/gotd/ige
	go get -u github.com/hrissan/tdigest
	# github.com/mailru/easyjson
	go get -u github.com/petar/GoLLRB
	go get -u github.com/pierrec/lz4
	go get -u github.com/pkg/errors
	go get -u github.com/spf13/pflag
	go get -u github.com/stretchr/testify
	# github.com/tinylib/msgp
	go get -u github.com/xi2/xz
	go get -u github.com/zeebo/xxh3
	go get -u go.uber.org/atomic
	go get -u go.uber.org/multierr
	go get -u go4.org/mem
	go get -u golang.org/x/crypto golang.org/x/exp golang.org/x/sync golang.org/x/sys
	# google.golang.org/protobuf
	# gopkg.in/yaml.v2
	go get -u pgregory.net/rand
	go get -u pgregory.net/rapid
	go get go@1.24
	# updating those monsters breaks tons of packages
	# go get -u github.com/grafana/grafana-plugin-sdk-go
	# go get -u k8s.io/apimachinery
	# go get -u github.com/prometheus/common github.com/prometheus/procfs github.com/prometheus/prometheus
	# go get -u github.com/testcontainers/testcontainers-go go get -u github.com/testcontainers/testcontainers-go/modules/clickhouse
