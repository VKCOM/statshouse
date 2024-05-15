#!/bin/bash

SCRIPT_DIR=$(dirname "$0")
BUILD_ROOT=$SCRIPT_DIR/../..

docker build -t localrun-agent -f $SCRIPT_DIR/agent.Dockerfile $BUILD_ROOT
