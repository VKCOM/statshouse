#!/bin/bash

SCRIPT_DIR=$(dirname "$0")
BUILD_ROOT=$SCRIPT_DIR/../..

docker build -t localrun-statshouse-bin \
 -f $SCRIPT_DIR/statshouse-bin.Dockerfile \
 --build-arg BUILD_TRUSTED_SUBNET_GROUPS="0.0.0.0/0" \
 $BUILD_ROOT
