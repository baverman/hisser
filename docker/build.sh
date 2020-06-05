#!/bin/sh
hisser_version=${1:-0.16.2}
graphite_version=${2:-1.1.4}
name=${3:-baverman/graphite-hisser}
tag=${4:-$graphite_version-$hisser_version-1}

cd $(dirname $0)
pushd ..
files="$(git ls-files hisser) README.rst LICENSE setup.py"
popd

if [ -n "$PROXY" ]; then
    proxy_opts="--build-arg=http_proxy=$PROXY --build-arg=HTTP_PROXY=$PROXY"
fi

tar cf - . -C .. $files | docker build \
  $proxy_opts --network=host \
  --build-arg=GRAPHITE_VERSION=$graphite_version \
  -t $name:$tag -t $name:latest -

if [ "$PUSH" ]; then
    docker push $name:$tag
fi

if [ "$PUSH" = "latest" ]; then
    docker push $name:latest
fi
