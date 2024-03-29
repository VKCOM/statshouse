#!/bin/bash

SCRIPT_DIR=$(dirname "$0")
BUILD_ROOT=$SCRIPT_DIR/../..

docker build -t localrun-statshouse-aggregator -f $SCRIPT_DIR/aggregator.Dockerfile $BUILD_ROOT
