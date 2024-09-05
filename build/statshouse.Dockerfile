FROM golang:1.21-bullseye AS build
WORKDIR /src
COPY go.mod go.sum Makefile ./
RUN go mod download -x
# ARG BUILD_TIME
# ARG BUILD_MACHINE
# ARG BUILD_COMMIT
# ARG BUILD_COMMIT_TS
# ARG BUILD_ID
# ARG BUILD_VERSION
ARG BUILD_TRUSTED_SUBNET_GROUPS
# ENV BUILD_TIME=$BUILD_TIME
# ENV BUILD_MACHINE=$BUILD_MACHINE
# ENV BUILD_COMMIT=$BUILD_COMMIT
# ENV BUILD_COMMIT_TS=$BUILD_COMMIT_TS
# ENV BUILD_ID=$BUILD_ID
# ENV BUILD_VERSION=$BUILD_VERSION
ENV BUILD_TRUSTED_SUBNET_GROUPS=$BUILD_TRUSTED_SUBNET_GROUPS
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN make build-sh

FROM gcr.io/distroless/base-debian11:nonroot
WORKDIR /var/lib/statshouse/cache/aggregator
WORKDIR /var/lib/statshouse/cache/agent
WORKDIR /home/nonroot
COPY build/statshouse_env.yml /etc/statshouse_env.yml
COPY --from=build /src/target/statshouse /bin
ENTRYPOINT ["/bin/statshouse"]
