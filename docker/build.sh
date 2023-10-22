#!/bin/sh
set -e
hisser_version=${1:-0.21.dev3}
name=${2:-baverman/graphite-hisser}
tag=${3:-$hisser_version-1}

cd $(dirname $0)
pushd ..
files="$(git ls-files hisser) README.rst LICENSE setup.py requirements.txt"
popd

if [ -n "$PROXY" ]; then
    proxy_opts="--build-arg=http_proxy=$PROXY --build-arg=HTTP_PROXY=$PROXY"
fi

tar cf - . -C .. $files | docker build \
  $proxy_opts --network=host \
  -t $name:$tag -t $name:latest -

docker run --rm $name:$tag python3 -m hisser.wsgi
docker run --rm $name:$tag python3 -m hisser --help

if [ "$PUSH" ]; then
    docker push $name:$tag
fi

if [ "$PUSH" = "latest" ]; then
    docker push $name:latest
fi
