#! /bin/bash
set -ex

: ${DOCKER_REPOSITORY?"You must export DOCKER_REPOSITORY"}
: ${IMAGE_TAG?"You must export IMAGE_TAG"}

ROOT_DIR=$(dirname $0)/..
NAMESPACE=${NAMESPACE:-examples}

#${ROOT_DIR}/driver-pravega/uninstall.sh

helm upgrade --install --timeout 600 --wait --debug \
${NAMESPACE}-openmessaging-benchmarking \
--namespace ${NAMESPACE} \
--set image=${DOCKER_REPOSITORY}/openmessaging-benchmark:${IMAGE_TAG} \
${ROOT_DIR}/deployment/kubernetes/helm/benchmark \
$@
