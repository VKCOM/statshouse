FROM golang:1.19-bullseye AS build
ARG BUILD_TIME
ARG BUILD_MACHINE
ARG BUILD_COMMIT
ARG BUILD_COMMIT_TS
ARG BUILD_ID
ARG BUILD_VERSION
ARG BUILD_TRUSTED_SUBNET_GROUPS
ENV BUILD_TIME=$BUILD_TIME
ENV BUILD_MACHINE=$BUILD_MACHINE
ENV BUILD_COMMIT=$BUILD_COMMIT
ENV BUILD_COMMIT_TS=$BUILD_COMMIT_TS
ENV BUILD_ID=$BUILD_ID
ENV BUILD_VERSION=$BUILD_VERSION
ENV BUILD_TRUSTED_SUBNET_GROUPS=$BUILD_TRUSTED_SUBNET_GROUPS
RUN mkdir -p /var/lib/statshouse/metadata/binlog
WORKDIR /src
COPY go.mod go.sum Makefile ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN go mod download
RUN make build-sh-metadata
RUN target/statshouse-metadata --binlog-prefix "/var/lib/statshouse/metadata/binlog/bl" --create-binlog "0,1"

FROM gcr.io/distroless/base-debian11:nonroot
COPY --from=build /src/target/statshouse-metadata /bin/
COPY --from=build --chown=nonroot:nonroot /var/lib/statshouse/ /var/lib/statshouse/
ENTRYPOINT ["/bin/statshouse-metadata"]
