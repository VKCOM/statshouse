FROM node:18-bullseye AS build-node
WORKDIR /src/statshouse-ui
COPY statshouse-ui/package.json statshouse-ui/package-lock.json ./
RUN npm clean-install
# ARG BUILD_TIME
# ARG BUILD_VERSION
# ARG REACT_APP_BUILD_VERSION
# ENV BUILD_TIME=$BUILD_TIME
# ENV BUILD_VERSION=$BUILD_VERSION
# ENV REACT_APP_BUILD_VERSION=$REACT_APP_BUILD_VERSION
COPY statshouse-ui .
ENV NODE_ENV=production
RUN --mount=type=cache,target="/root/.npm" npm run build


FROM golang:1.22-bullseye AS build-go
WORKDIR /src
RUN apt-get update && apt-get install -y gnuplot-nox gnuplot-data libpango-1.0-0 libcairo2
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
RUN --mount=type=cache,target="/root/.cache/go-build" --mount=type=cache,target="/go" make build-sh-api

FROM gcr.io/distroless/base-debian11:nonroot
WORKDIR /var/lib/statshouse/cache/api
WORKDIR /home/nonroot
COPY --from=build-go /src/target/statshouse-api /bin/
COPY --from=build-node /src/statshouse-ui/build /usr/lib/statshouse-api/statshouse-ui/
ENTRYPOINT ["/bin/statshouse-api", "--static-dir=/usr/lib/statshouse-api/statshouse-ui/"]
