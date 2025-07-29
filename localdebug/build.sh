#!/bin/bash
set -e

pushd ..
make build-main-daemons
if [ -f "statshouse-ui/build/index.html" ]; then
    echo "UI already build"
else
    make build-sh-ui
fi
popd
