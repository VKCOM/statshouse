FROM golang:1.21-bullseye AS DELVE
RUN go install github.com/go-delve/delve/cmd/dlv@latest
RUN cp $(which dlv) /bin/dlv

FROM golang:1.21-bullseye
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
WORKDIR /home/u/github/statshouse
COPY go.mod go.sum Makefile .
COPY cmd cmd
COPY internal internal
RUN go mod download -x
RUN make build-sh build-sh-api build-sh-metadata
RUN mv target/statshouse /bin/statshouse
RUN mv target/statshouse-api /bin/statshouse-api
RUN mv target/statshouse-metadata /bin/statshouse-metadata
COPY --from=DELVE /bin/dlv /bin/dlv
