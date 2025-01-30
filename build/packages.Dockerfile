# syntax=docker/dockerfile:1

FROM node:18-bullseye AS build-node
ARG BUILD_TIME
ARG BUILD_VERSION
ARG REACT_APP_BUILD_VERSION
ENV BUILD_TIME=$BUILD_TIME
ENV BUILD_VERSION=$BUILD_VERSION
ENV REACT_APP_BUILD_VERSION=$REACT_APP_BUILD_VERSION
WORKDIR /src
COPY Makefile ./
COPY statshouse-ui/ ./statshouse-ui/
COPY grafana-plugin-ui/ ./grafana-plugin-ui/
RUN make build-sh-ui build-grafana-ui

FROM golang:1.22-bullseye AS build-go-bullseye
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download -x
COPY Makefile ./
ARG BUILD_TIME
ARG BUILD_MACHINE
ARG BUILD_COMMIT
ARG BUILD_COMMIT_TS
ARG BUILD_ID
ARG BUILD_VERSION
ARG BUILD_TRUSTED_SUBNET_GROUPS
ARG GOCACHE
ENV BUILD_TIME=$BUILD_TIME
ENV BUILD_MACHINE=$BUILD_MACHINE
ENV BUILD_COMMIT=$BUILD_COMMIT
ENV BUILD_COMMIT_TS=$BUILD_COMMIT_TS
ENV BUILD_ID=$BUILD_ID
ENV BUILD_VERSION=$BUILD_VERSION
ENV BUILD_TRUSTED_SUBNET_GROUPS=$BUILD_TRUSTED_SUBNET_GROUPS
RUN --mount=type=bind,src=$GOCACHE,target=/root/.cache/go-build,readwrite \
    --mount=type=bind,src=internal/,target=/src/internal,readonly \
    --mount=type=bind,src=cmd/,target=/src/cmd,readonly \
    make build-sh build-sh-metadata build-sh-api build-sh-grafana build-igp build-agg

FROM golang:1.22-bookworm AS build-go-bookworm
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download -x
COPY Makefile ./
ARG BUILD_TIME
ARG BUILD_MACHINE
ARG BUILD_COMMIT
ARG BUILD_COMMIT_TS
ARG BUILD_ID
ARG BUILD_VERSION
ARG BUILD_TRUSTED_SUBNET_GROUPS
ARG GOCACHE
ENV BUILD_TIME=$BUILD_TIME
ENV BUILD_MACHINE=$BUILD_MACHINE
ENV BUILD_COMMIT=$BUILD_COMMIT
ENV BUILD_COMMIT_TS=$BUILD_COMMIT_TS
ENV BUILD_ID=$BUILD_ID
ENV BUILD_VERSION=$BUILD_VERSION
ENV BUILD_TRUSTED_SUBNET_GROUPS=$BUILD_TRUSTED_SUBNET_GROUPS
RUN --mount=type=bind,src=$GOCACHE,target=/root/.cache/go-build,readwrite \
    --mount=type=bind,src=internal/,target=/src/internal,readonly \
    --mount=type=bind,src=cmd/,target=/src/cmd,readonly \
    make build-sh build-sh-metadata build-sh-api build-sh-grafana build-igp build-agg

FROM golang:1.22-buster AS build-go-buster
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download -x
COPY Makefile ./
ARG BUILD_TIME
ARG BUILD_MACHINE
ARG BUILD_COMMIT
ARG BUILD_COMMIT_TS
ARG BUILD_ID
ARG BUILD_VERSION
ARG BUILD_TRUSTED_SUBNET_GROUPS
ARG GOCACHE
ENV BUILD_TIME=$BUILD_TIME
ENV BUILD_MACHINE=$BUILD_MACHINE
ENV BUILD_COMMIT=$BUILD_COMMIT
ENV BUILD_COMMIT_TS=$BUILD_COMMIT_TS
ENV BUILD_ID=$BUILD_ID
ENV BUILD_VERSION=$BUILD_VERSION
ENV BUILD_TRUSTED_SUBNET_GROUPS=$BUILD_TRUSTED_SUBNET_GROUPS
RUN --mount=type=bind,src=$GOCACHE,target=/root/.cache/go-build,readwrite \
    --mount=type=bind,src=internal/,target=/src/internal,readonly \
    --mount=type=bind,src=cmd/,target=/src/cmd,readonly \
    make build-sh build-sh-metadata build-sh-api build-sh-grafana build-igp build-agg

FROM golang:1.22-focal AS build-go-focal
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download -x
COPY Makefile ./
ARG BUILD_TIME
ARG BUILD_MACHINE
ARG BUILD_COMMIT
ARG BUILD_COMMIT_TS
ARG BUILD_ID
ARG BUILD_VERSION
ARG BUILD_TRUSTED_SUBNET_GROUPS
ARG GOCACHE
ENV BUILD_TIME=$BUILD_TIME
ENV BUILD_MACHINE=$BUILD_MACHINE
ENV BUILD_COMMIT=$BUILD_COMMIT
ENV BUILD_COMMIT_TS=$BUILD_COMMIT_TS
ENV BUILD_ID=$BUILD_ID
ENV BUILD_VERSION=$BUILD_VERSION
ENV BUILD_TRUSTED_SUBNET_GROUPS=$BUILD_TRUSTED_SUBNET_GROUPS
RUN --mount=type=bind,src=$GOCACHE,target=/root/.cache/go-build,readwrite \
    --mount=type=bind,src=internal/,target=/src/internal,readonly \
    --mount=type=bind,src=cmd/,target=/src/cmd,readonly \
    make build-sh build-sh-metadata build-sh-api build-sh-grafana build-igp build-agg

FROM golang:1.22-jammy AS build-go-jammy 
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download -x
COPY Makefile ./
ARG BUILD_TIME
ARG BUILD_MACHINE
ARG BUILD_COMMIT
ARG BUILD_COMMIT_TS
ARG BUILD_ID
ARG BUILD_VERSION
ARG BUILD_TRUSTED_SUBNET_GROUPS
ARG GOCACHE
ENV BUILD_TIME=$BUILD_TIME
ENV BUILD_MACHINE=$BUILD_MACHINE
ENV BUILD_COMMIT=$BUILD_COMMIT
ENV BUILD_COMMIT_TS=$BUILD_COMMIT_TS
ENV BUILD_ID=$BUILD_ID
ENV BUILD_VERSION=$BUILD_VERSION
ENV BUILD_TRUSTED_SUBNET_GROUPS=$BUILD_TRUSTED_SUBNET_GROUPS
RUN --mount=type=bind,src=$GOCACHE,target=/root/.cache/go-build,readwrite \
    --mount=type=bind,src=internal/,target=/src/internal,readonly \
    --mount=type=bind,src=cmd/,target=/src/cmd,readonly \
    make build-sh build-sh-metadata build-sh-api build-sh-grafana build-igp build-agg

FROM debian:bullseye AS debuild-bullseye
ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked apt-get update \
  && apt-get install -y devscripts build-essential dh-exec \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build-go-bullseye /src/target/* /src/target/
COPY --from=build-node /src/statshouse-ui/build/ /src/statshouse-ui/build/
COPY --from=build-node /src/grafana-plugin-ui/dist /src/grafana-plugin-ui/dist/
COPY build/debian/ /src/build/debian/
WORKDIR /src/build
ARG DEBIAN_VERSION
RUN dch --create --distribution stable --package statshouse --newversion "$DEBIAN_VERSION" "up to version $DEBIAN_VERSION"
RUN --mount=type=bind,src=cmd/,target=/src/cmd,readonly debuild --no-lintian -us -uc -b

FROM debian:bookworm AS debuild-bookworm
ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked apt-get update \
  && apt-get install -y devscripts build-essential dh-exec \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build-go-bookworm /src/target/* /src/target/
COPY --from=build-node /src/statshouse-ui/build/ /src/statshouse-ui/build/
COPY --from=build-node /src/grafana-plugin-ui/dist /src/grafana-plugin-ui/dist/
COPY build/debian/ /src/build/debian/
WORKDIR /src/build
ARG DEBIAN_VERSION
RUN dch --create --distribution stable --package statshouse --newversion "$DEBIAN_VERSION" "up to version $DEBIAN_VERSION"
RUN --mount=type=bind,src=cmd/,target=/src/cmd,readonly debuild --no-lintian -us -uc -b

FROM debian:buster AS debuild-buster
ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked apt-get update \
  && apt-get install -y devscripts build-essential dh-exec \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build-go-buster /src/target/* /src/target/
COPY --from=build-node /src/statshouse-ui/build/ /src/statshouse-ui/build/
COPY --from=build-node /src/grafana-plugin-ui/dist /src/grafana-plugin-ui/dist/
COPY build/debian/ /src/build/debian/
WORKDIR /src/build
ARG DEBIAN_VERSION
RUN dch --create --distribution stable --package statshouse --newversion "$DEBIAN_VERSION" "up to version $DEBIAN_VERSION"
RUN --mount=type=bind,src=cmd/,target=/src/cmd,readonly debuild --no-lintian -us -uc -b

FROM ubuntu:focal AS debuild-focal
ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked apt-get update \
  && apt-get install -y devscripts build-essential dh-exec \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build-go-focal /src/target/* /src/target/
COPY --from=build-node /src/statshouse-ui/build/ /src/statshouse-ui/build/
COPY --from=build-node /src/grafana-plugin-ui/dist /src/grafana-plugin-ui/dist/
COPY build/debian/ /src/build/debian/
WORKDIR /src/build
ARG DEBIAN_VERSION
RUN dch --create --distribution stable --package statshouse --newversion "$DEBIAN_VERSION" "up to version $DEBIAN_VERSION"
RUN --mount=type=bind,src=cmd/,target=/src/cmd,readonly debuild --no-lintian -us -uc -b

FROM ubuntu:jammy AS debuild-jammy 
ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked apt-get update \
  && apt-get install -y devscripts build-essential dh-exec \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build-go-jammy /src/target/* /src/target/
COPY --from=build-node /src/statshouse-ui/build/ /src/statshouse-ui/build/
COPY --from=build-node /src/grafana-plugin-ui/dist /src/grafana-plugin-ui/dist/
COPY build/debian/ /src/build/debian/
WORKDIR /src/build
ARG DEBIAN_VERSION
RUN dch --create --distribution stable --package statshouse --newversion "$DEBIAN_VERSION" "up to version $DEBIAN_VERSION"
RUN --mount=type=bind,src=cmd/,target=/src/cmd,readonly debuild --no-lintian -us -uc -b

FROM scratch AS debian-bullseye
COPY --from=debuild-bullseye /src/*.deb /

FROM scratch AS debian-bookworm
COPY --from=debuild-bookworm /src/*.deb /

FROM scratch AS debian-buster
COPY --from=debuild-buster /src/*.deb /

FROM scratch AS ubuntu-focal
COPY --from=debuild-focal /src/*.deb /

FROM scratch AS ubuntu-jammy
COPY --from=debuild-jammy /src/*.deb /
