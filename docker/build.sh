#!/bin/sh
hisser_version=${1:-0.16.1}
graphite_version=${2:-1.1.4}
name=${3:-baverman/graphite-hisser}
tag=${4:-$graphite_version-$hisser_version-1}

cd $(dirname $0)
pushd ..
files="$(git ls-files hisser) README.rst LICENSE setup.py"
popd

tar cf - . -C .. $files | docker build --build-arg=GRAPHITE_VERSION=$graphite_version \
                                       -t $name:$tag -t $name:latest -

if [ "$PUSH" ]; then
    docker push $name:$tag
fi

if [ "$PUSH" = "latest" ]; then
    docker push $name:latest
fi
