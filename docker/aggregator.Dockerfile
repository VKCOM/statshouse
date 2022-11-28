FROM golang:1.19-bullseye AS build
RUN mkdir -p /var/lib/statshouse/cache/aggregator
WORKDIR /src
COPY go.mod go.sum Makefile ./
COPY .git/ ./.git/
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN go mod download
RUN make build-sh

FROM yandex/clickhouse-server:21.8.9
RUN groupadd kitten
RUN useradd -g kitten kitten
COPY --from=build /src/target/statshouse /bin/
COPY --from=build --chown=kitten:kitten /var/lib/statshouse/ /var/lib/statshouse/
COPY docker/key1.txt /etc/statshouse/
COPY docker/aggregator-entrypoint.sh /bin/
COPY docker/clickhouse.sql /docker-entrypoint-initdb.d/
ENTRYPOINT ["/bin/aggregator-entrypoint.sh"]
