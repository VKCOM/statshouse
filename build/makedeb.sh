#!/bin/bash
set -ex

if [[ $1 ]]; then
  TAG=$1 # tag provided in command line overrides environment variable
fi

if [[ -z $TAG ]]; then
  echo "Debian code name isn't specified! Expected jessie, stretch, buster or bullseye."
  exit 1
fi

if [[ -z $BUILD_VERSION ]]; then
  # upstream-version
  UPSTREAM=$(git describe --tags --always --dirty)
  UPSTREAM=${UPSTREAM#v} # v1.0.0 -> 1.0.0
  # epoch:upstream-version-debian.revision
  BUILD_VERSION="1:$UPSTREAM-$TAG"
fi

if [[ -z $GID ]]; then
  GID=$(id -g)
fi

# build StatsHouse
GOLANG_IMAGE="golang:1.19-$TAG" # e.g. golang:1.19-bullseye
if [[ $CUSTOM_BUILD_IMAGE ]]; then
  GOLANG_IMAGE="statshouse-build-$CUSTOM_BUILD_IMAGE:$TAG"
  docker build -t "$GOLANG_IMAGE" --build-arg PARENT="$CUSTOM_BUILD_IMAGE:$TAG" \
    --build-arg GOLANG_URL="$GOLANG_URL" --build-arg GOLANG_SHA256="$GOLANG_SHA256" \
    - < build/golang.Dockerfile
fi
GOCACHE=build/go-cache
mkdir -p "$PWD/$GOCACHE"
COMMON_BUILD_VARS=("-e=BUILD_TIME" "-e=BUILD_MACHINE" "-e=BUILD_COMMIT" "-e=BUILD_VERSION" "-e=BUILD_ID" \
                   "-e=BUILD_COMMIT_TS" "-e=BUILD_TRUSTED_SUBNET_GROUPS")
docker run --rm -v "$PWD:/src" -w /src "${COMMON_BUILD_VARS[@]}" -e GOCACHE="/src/$GOCACHE" \
   -u "$UID:$GID" "$GOLANG_IMAGE" make build-sh build-sh-metadata build-sh-api build-sh-grafana
docker run --rm -v "$PWD:/src" -w /src -u "$UID:$GID" node:18-bullseye make build-sh-ui build-grafana-ui

# build debian package
(cd build
rm -f debian/changelog
DEB_IMAGE="statshouse-build-deb"
docker build -t "$DEB_IMAGE" - < debuild.Dockerfile
docker run --rm -v "$PWD:/src" -w /src -u "$UID:$GID" "$DEB_IMAGE" dch \
  --create --distribution stable --package statshouse \
  --newversion "$BUILD_VERSION" "up to version $BUILD_VERSION"
docker run --rm -v "$PWD/..:/src" -w /src/build -u "$UID:$GID" "$DEB_IMAGE" debuild --no-lintian -us -uc -b)

# Drop to target directory
for f in *"${BUILD_VERSION##[0-9]*\:}"*; do mv -u "$f" "target/$f"; done
