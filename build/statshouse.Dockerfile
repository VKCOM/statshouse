FROM golang:1.19-bullseye AS build
ARG BUILD_TRUSTED_SUBNET_GROUPS
ENV BUILD_TRUSTED_SUBNET_GROUPS=$BUILD_TRUSTED_SUBNET_GROUPS
RUN mkdir -p /var/lib/statshouse/cache/aggregator
RUN mkdir -p /var/lib/statshouse/cache/agent
WORKDIR /src
COPY go.mod go.sum Makefile ./
COPY .git/ ./.git/
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN go mod download
RUN make build-sh

FROM gcr.io/distroless/base-debian11:nonroot
COPY --from=build /src/target/statshouse /bin/
COPY --from=build --chown=nonroot:nonroot /var/lib/statshouse/ /var/lib/statshouse/
ENTRYPOINT ["/bin/statshouse"]
