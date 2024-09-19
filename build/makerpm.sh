#!/bin/bash
set -e

while (( "$#" )); do

IMAGE=$1
IMAGE_NAME=${IMAGE%:*}
IMAGE_TAG=${IMAGE#*:}

case "$IMAGE_NAME" in
  "almalinux") NAME_RELEASE="almalinux$IMAGE_TAG"; DNF=dnf ;;
  "centos") NAME_RELEASE=$IMAGE_TAG; DNF=yum ;;
  *) echo "Target name \"$IMAGE_NAME\" is not recognized!"; shift; continue ;;
esac

case "$IMAGE_TAG" in
  "centos6") DNF_GROUP_CMD="groupinstall" ;;
  *) DNF_GROUP_CMD="group install" ;;
esac 

NAME_RELEASE=$(echo $NAME_RELEASE | sed -e 's/-/./g;s/:/./g')

if [[ -z $GOLANG_VERSION ]]; then
  GOLANG_VERSION='1.21.9'
  echo "Go version is not specified, using $GOLANG_VERSION"
fi

# version
UPSTREAM=$(git describe --tags --always --dirty)
UPSTREAM=${UPSTREAM#v} # v1.0.0 -> 1.0.0
BUILD_TIME=$(date +%FT%T%z)
REACT_APP_BUILD_VERSION=$UPSTREAM-$BUILD_TIME
if [[ -z $BUILD_VERSION ]]; then
  if [[ ! -z $BUILD_VERSION_SUFFIX ]]; then
    BUILD_VERSION=$UPSTREAM-$BUILD_VERSION_SUFFIX
  elif [[ ! -z $TAG ]]; then
    BUILD_VERSION=$UPSTREAM-$TAG
  else
    BUILD_VERSION=$UPSTREAM
  fi
fi

if [[ -z $GID ]]; then
  GID=$(id -g)
fi

# builder image
BUILD_IMAGE=statshouse_builder_$NAME_RELEASE
if [ $IMAGE_NAME = 'centos' ]; then
  docker image build -t $BUILD_IMAGE - <<EOF
FROM $IMAGE
RUN sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/CentOS-*.repo
RUN sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/CentOS-*.repo
RUN sed -i s/^mirrorlist=http/#mirrorlist=http/g /etc/yum.repos.d/CentOS-*.repo
RUN $DNF update -y
RUN $DNF $DNF_GROUP_CMD -y "Development Tools"
RUN  curl -L https://go.dev/dl/go$GOLANG_VERSION.linux-amd64.tar.gz | tar -C /usr/local -xzf -
ENV PATH=\$PATH:/usr/local/go/bin
RUN groupadd -g $GID builder
RUN useradd -u $UID -g $GID builder
EOF
else
  docker image build -t $BUILD_IMAGE - <<EOF
FROM $IMAGE
RUN $DNF update -y
RUN $DNF $DNF_GROUP_CMD -y "Development Tools"
RUN  curl -L https://go.dev/dl/go$GOLANG_VERSION.linux-amd64.tar.gz | tar -C /usr/local -xzf -
ENV PATH=\$PATH:/usr/local/go/bin
RUN groupadd -g $GID builder
RUN useradd -u $UID -g $GID builder
EOF
fi

# frontend
if [ ! -d statshouse-ui/build ]; then
  if [[ -z $NODE_IMAGE ]]; then
    NODE_IMAGE="node:18-bullseye"
    echo "Node image is not specified, using $NODE_IMAGE"
  fi
  docker run --rm -u "$UID:$GID" -v "$PWD:/src" -w /src -e REACT_APP_BUILD_VERSION="$REACT_APP_BUILD_VERSION" $NODE_IMAGE make build-sh-ui
fi

# backend & RPM
GOCACHE=build/go-cache
mkdir -p "$PWD/$GOCACHE"
docker run -i --rm -u "$UID:$GID" -v "$PWD:/src" -w /src \
  -e BUILD_MACHINE="$(uname -n -m -r -s)" \
  -e BUILD_VERSION="$UPSTREAM" \
  -e BUILD_COMMIT="$(git log --format="%H" -n 1)" \
  -e BUILD_COMMIT_TS="$(git log --format="%ct" -n 1)" \
  -e GOCACHE="/src/$GOCACHE" -e BUILD_TRUSTED_SUBNET_GROUPS \
  -e BUILD_ID \
  -e BUILD_TIME \
  $BUILD_IMAGE /bin/bash <<EOF
  set -x &&\
  make build-sh build-sh-metadata build-sh-api &&\
  mkdir -p BUILD BUILDROOT SOURCES SPECS SRPMS RPMS &&\
  cp build/statshouse.spec SPECS/statshouse.spec &&\
  BUILD_VERSION=\$(echo $BUILD_VERSION | sed -e 's:-:.:g') &&\
  rpmbuild --define "_topdir \$(pwd)" --define "BUILD_VERSION \$BUILD_VERSION" --define "OS_NAME_RELEASE $NAME_RELEASE" -bb SPECS/statshouse.spec &&\
  rm -rf BUILD BUILDROOT SOURCES SPECS SRPMS &&\
  true
EOF

shift
done
