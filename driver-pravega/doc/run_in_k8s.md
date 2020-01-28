# Run in Kubernetes

## Build Docker container

```
./docker-build.sh
```

## Run local driver on Kubernetes:
```
kubectl run -n examples --rm -it --image claudiofahey/openmessaging-benchmark:latest --serviceaccount examples-pravega openmessaging-benchmark
```

## Run in Kubernetes

```
./deploy-k8s-components.sh
