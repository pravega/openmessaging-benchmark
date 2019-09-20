# Pravega Driver for OpenMessaging Benchmark

```
./docker-build.sh
./deploy-k8s-components.sh
```

Run local driver on Kubernetes:
```
kubectl run -n examples --rm -it --image claudiofahey/openmessaging-benchmark:latest --serviceaccount examples-pravega openmessaging-benchmark
```
