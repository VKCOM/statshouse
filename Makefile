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

.PHONY: all build-go build-ui build-docker \
	build-sh build-sh-api build-sh-api-embed build-sh-metadata build-sh-grafana \
	build-sh-ui build-grafana-ui \
	build-docker-sh build-docker-sh-api build-docker-sh-metadata \
	copy-sh-ui

all: build-go build-ui build-docker
build-go: build-sh build-sh-api build-sh-metadata build-sh-grafana
build-ui: build-sh-ui build-grafana-ui
build-docker: build-docker-sh build-docker-sh-api build-docker-sh-metadata

build-sh:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse ./cmd/statshouse

build-sh-api:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-api ./cmd/statshouse-api

build-sh-api-embed: copy-sh-ui
	go build -tags embed -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-api ./cmd/statshouse-api

build-sh-metadata:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-metadata ./cmd/statshouse-metadata

build-sh-grafana:
	go build -ldflags "$(COMMON_LDFLAGS)" -buildvcs=false -o target/statshouse-grafana-plugin ./cmd/statshouse-grafana-plugin

build-sh-ui:
	cd statshouse-ui && npm clean-install && NODE_ENV=production REACT_APP_BUILD_VERSION=$(REACT_APP_BUILD_VERSION) npm run build

copy-sh-ui:
	cp -r statshouse-ui/build cmd/statshouse-api/

build-grafana-ui:
	cd grafana-plugin-ui && npm clean-install && npm run build

build-docker-sh:
	docker build --build-arg BUILD_TIME="$(BUILD_TIME)" --build-arg BUILD_MACHINE="$(BUILD_MACHINE)" \
		--build-arg BUILD_COMMIT="$(BUILD_COMMIT)" --build-arg BUILD_COMMIT_TS="$(BUILD_COMMIT_TS)" \
		--build-arg BUILD_ID="$(BUILD_ID)" --build-arg BUILD_VERSION="$(BUILD_VERSION)" \
		--build-arg BUILD_TRUSTED_SUBNET_GROUPS="$(BUILD_TRUSTED_SUBNET_GROUPS)" \
		-t statshouse -f build/statshouse.Dockerfile .

build-docker-sh-api:
	docker build --build-arg BUILD_TIME="$(BUILD_TIME)" --build-arg BUILD_MACHINE="$(BUILD_MACHINE)" \
		--build-arg BUILD_COMMIT="$(BUILD_COMMIT)" --build-arg BUILD_COMMIT_TS="$(BUILD_COMMIT_TS)" \
		--build-arg BUILD_ID="$(BUILD_ID)" --build-arg BUILD_VERSION="$(BUILD_VERSION)" \
		--build-arg BUILD_TRUSTED_SUBNET_GROUPS="$(BUILD_TRUSTED_SUBNET_GROUPS)" \
		--build-arg REACT_APP_BUILD_VERSION="$(REACT_APP_BUILD_VERSION)" \
		-t statshouse-api -f build/statshouse-api.Dockerfile .

build-docker-sh-metadata:
	docker build --build-arg BUILD_TIME="$(BUILD_TIME)" --build-arg BUILD_MACHINE="$(BUILD_MACHINE)" \
		--build-arg BUILD_COMMIT="$(BUILD_COMMIT)" --build-arg BUILD_COMMIT_TS="$(BUILD_COMMIT_TS)" \
		--build-arg BUILD_ID="$(BUILD_ID)" --build-arg BUILD_VERSION="$(BUILD_VERSION)" \
		--build-arg BUILD_TRUSTED_SUBNET_GROUPS="$(BUILD_TRUSTED_SUBNET_GROUPS)" \
		-t statshouse-metadata -f build/statshouse-metadata.Dockerfile .

build-deb:
	./build/makedeb.sh

# if tlgen is not installed, replace tlgen with full path, for example ~/go/src/gitlab.mvk.com/go/vkgo/projects/vktl/cmd/tlgen/tlgen
.PHONY: gen
gen:
	@tlgen --outdir=./internal/data_model/gen2 -v \
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

.PHONY: lint
lint:
	staticcheck -version
	staticcheck ./...

