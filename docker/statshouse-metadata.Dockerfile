FROM golang:1.19-bullseye AS build
RUN mkdir -p /var/lib/statshouse/metadata/binlog
WORKDIR /src
COPY go.mod go.sum Makefile ./
COPY .git/ ./.git/
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN go mod download
RUN make build-sh-metadata
RUN target/statshouse-metadata --binlog-prefix "/var/lib/statshouse/metadata/binlog/bl" --create-binlog "0,1"

FROM gcr.io/distroless/base-debian11:nonroot
COPY --from=build /src/target/statshouse-metadata /bin/
COPY --from=build --chown=nonroot:nonroot /var/lib/statshouse/ /var/lib/statshouse/
COPY docker/key1.txt /etc/statshouse/
ENTRYPOINT ["/bin/statshouse-metadata"]
