#! /bin/bash
set -x

ROOT_DIR=$(dirname $0)/..
NAMESPACE=${NAMESPACE:-examples}

helm  del --purge \
${NAMESPACE}-openmessaging-benchmarking

kubectl wait --for=delete --timeout=300s statefulset/${NAMESPACE}-openmessaging-benchmarking-worker -n ${NAMESPACE}
kubectl wait --for=delete --timeout=300s pod/${NAMESPACE}-openmessaging-benchmarking-driver -n ${NAMESPACE}

true
