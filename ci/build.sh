#!/bin/bash
set -e
image=${1:?Python image is required}
pyver=$(basename $image)

if [ -n "$PROXY" ]; then
    proxy_opts="--build-arg=http_proxy=$PROXY --build-arg=HTTP_PROXY=$PROXY"
fi

test -t 1 && DTTY='-t'

docker build --network=host $proxy_opts --build-arg IMAGE=$image -t hisser-$pyver -f ci/Dockerfile.test ci
docker run $DTTY --rm -w /build -u $UID:$GROUPS -v $PWD:/build hisser-$pyver sh -c "python setup.py build_ext --inplace && py.test"
