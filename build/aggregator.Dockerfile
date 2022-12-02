FROM golang:1.19-bullseye AS build
RUN mkdir -p /var/lib/statshouse/cache/aggregator
WORKDIR /src
COPY go.mod go.sum Makefile ./
COPY .git/ ./.git/
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN go mod download
RUN make build-sh

FROM clickhouse/clickhouse-server:22.11
RUN groupadd kitten
RUN useradd -g kitten kitten
COPY --from=build /src/target/statshouse /bin/
COPY --from=build --chown=kitten:kitten /var/lib/statshouse/ /var/lib/statshouse/
COPY build/key1.txt /etc/statshouse/
COPY build/aggregator-entrypoint.sh /bin/
COPY build/clickhouse.sql /docker-entrypoint-initdb.d/
ENTRYPOINT ["/bin/aggregator-entrypoint.sh"]
