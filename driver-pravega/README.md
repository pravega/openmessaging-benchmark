# Pravega Driver for OpenMessaging Benchmark

```
mvn install
export BENCHMARK_TARBALL=package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz
docker build --build-arg BENCHMARK_TARBALL . -f docker/Dockerfile -t openmessaging-benchmark:latest
docker run --rm -it --network host openmessaging-benchmark:latest
```
