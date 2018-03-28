#!/bin/bash
set -e
image=${1:?Python image is required}
pyver=$(basename $image)
docker build --build-arg IMAGE=$image -t hisser-$pyver -f ci/Dockerfile.test ci
docker run --rm -w /build -u $UID:$GROUPS -v $PWD:/build hisser-$pyver py.test
