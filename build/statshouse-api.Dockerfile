FROM node:18-bullseye AS build-node
WORKDIR /src
COPY Makefile ./
COPY .git/ ./.git/
COPY statshouse-ui/ ./statshouse-ui/
RUN make build-sh-ui

FROM golang:1.19-bullseye AS build-go
WORKDIR /src
COPY go.mod go.sum Makefile ./
COPY .git/ ./.git/
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY --from=build-node /src/statshouse-ui/build /src/statshouse-ui/build/
RUN go mod download
RUN make build-sh-api-embed
RUN mkdir -p /var/lib/statshouse/cache/api

FROM gcr.io/distroless/base-debian11:nonroot
COPY --from=build-go /src/target/statshouse-api /bin/
COPY --from=build-go --chown=nonroot:nonroot /var/lib/statshouse/ /var/lib/statshouse/
COPY build/key1.txt /etc/statshouse/
ENTRYPOINT ["/bin/statshouse-api"]
