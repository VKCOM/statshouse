BUILD_VERSION    := $(shell git describe --tags --always --dirty)
BUILD_COMMIT     := $(shell git log --format="%H" -n 1)
BUILD_COMMIT_TS  := $(shell git log --format="%ct" -n 1)
BUILD_TIME       := $(shell date +%FT%T%z)
BUILD_MACHINE    := $(shell uname -n -m -r -s)
# TODO: BUILD_ID

COMMON_BUILD_VARS := -X 'github.com/vkcom/statshouse/internal/vkgo/build.time=$(BUILD_TIME)' \
	-X 'github.com/vkcom/statshouse/internal/vkgo/build.machine=$(BUILD_MACHINE)' \
	-X 'github.com/vkcom/statshouse/internal/vkgo/build.commit=$(BUILD_COMMIT)' \
	-X 'github.com/vkcom/statshouse/internal/vkgo/build.version=$(BUILD_VERSION)' \
	-X 'github.com/vkcom/statshouse/internal/vkgo/build.number=$(BUILD_ID)' \
	-X 'github.com/vkcom/statshouse/internal/vkgo/build.commitTimestamp=$(BUILD_COMMIT_TS)'

COMMON_LDFLAGS = $(COMMON_BUILD_VARS) -extldflags '-O2'

.PHONY: all build-go build-ui build-docker \
	build-sh build-sh-api build-sh-api-embed build-sh-metadata build-sh-grafana \
	build-sh-ui build-grafana-ui \
	build-docker-sh build-docker-sh-api build-docker-sh-metadata

all: build-go build-ui build-docker
build-go: build-sh build-sh-api build-sh-metadata build-sh-grafana
build-ui: build-sh-ui build-grafana-ui
build-docker: build-docker-sh build-docker-sh-api build-docker-sh-metadata

build-sh:
	go build -ldflags "$(COMMON_LDFLAGS)" -o target/statshouse ./cmd/statshouse

build-sh-api:
	go build -ldflags "$(COMMON_LDFLAGS)" -o target/statshouse-api ./cmd/statshouse-api

build-sh-api-embed:
	go build -tags embed -ldflags "$(COMMON_LDFLAGS)" -o target/statshouse-api ./cmd/statshouse-api

build-sh-metadata:
	go build -ldflags "$(COMMON_LDFLAGS)" -o target/statshouse-metadata ./cmd/statshouse-metadata

build-sh-grafana:
	go build -ldflags "$(COMMON_LDFLAGS)" -o target/statshouse-grafana-plugin ./cmd/statshouse-grafana-plugin

build-sh-ui:
	cd statshouse-ui && npm clean-install && NODE_ENV=production REACT_APP_BUILD_VERSION=$(BUILD_VERSION) npm run build

build-grafana-ui:
	cd grafana-plugin-ui && yarn build

build-docker-sh:
	docker build -t statshouse -f docker/statshouse.Dockerfile .

build-docker-sh-api:
	docker build -t statshouse-api -f docker/statshouse-api.Dockerfile .

build-docker-sh-metadata:
	docker build -t statshouse-metadata -f docker/statshouse-metadata.Dockerfile .
