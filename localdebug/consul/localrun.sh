#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$0")
BUILD_ROOT=$SCRIPT_DIR/../..

if [ -z "$(docker images -q localrun-statshouse-bin 2> /dev/null)" ]; then
  $SCRIPT_DIR/build-bin.sh
fi

if [ -z "$(docker images -q localrun-statshouse-ui 2> /dev/null)" ]; then
  docker build -t localrun-statshouse-ui -f $SCRIPT_DIR/statshouse-ui.Dockerfile $BUILD_ROOT
fi

if [ -z "$(docker images -q localrun-statshouse-aggregator 2> /dev/null)" ]; then
  $SCRIPT_DIR/build-aggregator.sh
fi

if [ -z "$(docker images -q localrun-agent 2> /dev/null)" ]; then
  $SCRIPT_DIR/build-agent.sh
fi

docker compose -f $SCRIPT_DIR/docker-compose.yml up --remove-orphans
