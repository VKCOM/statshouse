FROM golang:1.21-bullseye AS delve
RUN go install github.com/go-delve/delve/cmd/dlv@latest
RUN cp $(which dlv) /bin/dlv

FROM golang:1.21-bullseye
ENV BUILD_TRUSTED_SUBNET_GROUPS=0.0.0.0/0
WORKDIR /home/u/github/statshouse
COPY go.mod go.sum Makefile .
COPY cmd cmd
COPY internal internal
RUN go mod download -x
RUN make build-sh build-sh-api build-sh-metadata
RUN mv target/statshouse /bin/statshouse
RUN mv target/statshouse-api /bin/statshouse-api
RUN mv target/statshouse-metadata /bin/statshouse-metadata
COPY --from=delve /bin/dlv /bin/dlv
