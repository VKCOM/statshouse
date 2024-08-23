FROM node:18-bullseye
WORKDIR /src
COPY Makefile ./
COPY statshouse-ui/ ./statshouse-ui/
RUN make build-sh-ui
RUN mv /src/statshouse-ui/build /ui
