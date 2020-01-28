## Build Pravega

To use a pre-release version of Pravega, you must build it manually
using the following steps.

```
git clone https://github.com/pravega/pravega
cd pravega
git checkout master
./gradlew install distTar
```

This will build the file `pravega/build/distributions/pravega-0.7.0-2361.f273314-SNAPSHOT.tgz.`

If needed, change the variable `pravegaVersion` in `deploy.yaml` to match the version built.

If needed, change `pom.xml` to match the version built.

## Build Benchmark

```
mvn install
```
