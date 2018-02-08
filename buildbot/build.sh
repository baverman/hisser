#!/bin/bash
set -e
pyver=${1:?Version is required}
docker build --build-arg IMAGE=$pyver -t hisser-$pyver -f buildbot/Dockerfile.test buildbot
docker run --rm -w /build -u $UID:$GROUPS -v $PWD:/build hisser-$pyver py.test
