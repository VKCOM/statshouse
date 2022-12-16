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
RUN make build-sh-ui

FROM golang:1.19-bullseye AS build-go
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
WORKDIR /src
COPY go.mod go.sum Makefile ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN go mod download
RUN make build-sh build-sh-api build-sh-metadata

FROM clickhouse/clickhouse-server:22.11
RUN groupadd kitten
RUN useradd -g kitten kitten
RUN mkdir -p /var/lib/statshouse/cache/aggregator
RUN mkdir -p /var/lib/statshouse/cache/agent
RUN mkdir -p /var/lib/statshouse/cache/api
RUN mkdir -p /var/lib/statshouse/metadata/binlog
COPY build/clickhouse.sql /docker-entrypoint-initdb.d/
COPY build/all-in-one-entrypoint.sh /bin/
COPY --from=build-go /src/target/statshouse /bin/
COPY --from=build-go /src/target/statshouse-api /bin/
COPY --from=build-go /src/target/statshouse-metadata /bin/
COPY --from=build-node /src/statshouse-ui/build /usr/lib/statshouse-api/statshouse-ui/
RUN /bin/statshouse-metadata --binlog-prefix "/var/lib/statshouse/metadata/binlog/bl" --create-binlog "0,1"
RUN chown -R kitten:kitten /var/lib/statshouse
ENTRYPOINT ["/bin/all-in-one-entrypoint.sh"]
