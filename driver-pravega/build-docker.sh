#!/usr/bin/env bash
set -ex

: ${DOCKER_REPOSITORY?"You must export DOCKER_REPOSITORY"}
: ${IMAGE_TAG?"You must export IMAGE_TAG"}

ROOT_DIR=$(dirname $0)/..
cd ${ROOT_DIR}
mvn install
export BENCHMARK_TARBALL=package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz
docker build --build-arg BENCHMARK_TARBALL . -f docker/Dockerfile -t ${DOCKER_REPOSITORY}/openmessaging-benchmark:${IMAGE_TAG}
docker push ${DOCKER_REPOSITORY}/openmessaging-benchmark:${IMAGE_TAG}
