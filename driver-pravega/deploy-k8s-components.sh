#! /bin/bash
set -ex

ROOT_DIR=$(dirname $0)/..
NAMESPACE=${NAMESPACE:-examples}

#${ROOT_DIR}/driver-pravega/uninstall.sh

helm upgrade --install --timeout 600 --wait --debug \
${NAMESPACE}-openmessaging-benchmarking \
--namespace ${NAMESPACE} \
${ROOT_DIR}/deployment/kubernetes/helm/benchmark \
$@
