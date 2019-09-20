#!/usr/bin/env bash
set -ex
ROOT_DIR=$(dirname $0)/..
cd ${ROOT_DIR}
mvn install
export BENCHMARK_TARBALL=package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz
docker build --build-arg BENCHMARK_TARBALL . -f docker/Dockerfile -t claudiofahey/openmessaging-benchmark:latest
docker push claudiofahey/openmessaging-benchmark:latest
#kubectl run -n examples --rm -it --image claudiofahey/openmessaging-benchmark:latest --serviceaccount examples-pravega openmessaging-benchmark
