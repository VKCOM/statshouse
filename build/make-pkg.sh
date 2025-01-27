#!/bin/bash
set -ex

if [[ $1 ]]; then
  TAG=$1 # tag provided in command line overrides environment variable
fi

if [[ -z $TAG ]]; then
  echo "Distribution code name isn't specified! Expected one of: debian-bullseye, debian-bookworm, debian-buster, ubuntu-focal, ubuntu-jammy"
  exit 1
fi

# upstream-version
UPSTREAM=$(git describe --tags --always --dirty)
UPSTREAM=${UPSTREAM#v} # v1.0.0 -> 1.0.0
BUILD_TIME="$(date +%FT%T%z)"
REACT_APP_BUILD_VERSION=$UPSTREAM-$BUILD_TIME
if [[ -z $BUILD_VERSION ]]; then
  if [[ -z $BUILD_VERSION_SUFFIX ]]; then
    # epoch:upstream-version-debian.revision
    BUILD_VERSION="1:$UPSTREAM-$TAG"
  else
    BUILD_VERSION="1:$UPSTREAM-$BUILD_VERSION_SUFFIX"
  fi
fi
BUILD_MACHINE="$(uname -n -m -r -s)"
BUILD_COMMIT="$(git log --format="%H" -n 1)"
BUILD_COMMIT_TS="$(git log --format="%ct" -n 1)"

GOCACHE=$PWD/build/go-cache
mkdir -p "$GOCACHE"

if [[ $TAG == "ubuntu-focal" ]]; then
  docker build --file build/golang-ubuntu/golang-1.22-focal.Dockerfile --tag golang:1.22-focal build/golang-ubuntu
fi
if [[ $TAG == "ubuntu-jammy" ]]; then
  docker build --file build/golang-ubuntu/golang-1.22-jammy.Dockerfile --tag golang:1.22-jammy build/golang-ubuntu
fi
if [[ $TAG == "debian-buster" ]]; then
  docker build --file build/golang-debian/golang-1.22-buster.Dockerfile --tag golang:1.22-buster build/golang-debian
fi

docker build --file build/packages.Dockerfile \
    --build-arg BUILD_TIME \
    --build-arg BUILD_MACHINE \
    --build-arg BUILD_COMMIT=$BUILD_COMMIT \
    --build-arg BUILD_COMMIT_TS=$BUILD_COMMIT_TS \
    --build-arg BUILD_ID \
    --build-arg BUILD_VERSION=$UPSTREAM \
    --build-arg BUILD_TRUSTED_SUBNET_GROUPS \
    --build-arg DEBIAN_VERSION=$BUILD_VERSION \
    --build-arg GOCACHE \
    --output target/$TAG --target $TAG .

