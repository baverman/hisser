#!/bin/sh
name=${1:-baverman/graphite-hisser}
graphite_version=${2:-1.1.3}
hisser_version=${3:-0.10.1}
tag=${4:-$graphite_version-$hisser_version-2}

cd $(dirname $0)
pushd ..
files="$(git ls-files hisser) README.rst LICENSE setup.py"
popd

tar cf - . -C .. $files | docker build --build-arg=GRAPHITE_VERSION=$graphite_version \
                                       -t $name:$tag -t $name:latest -
