#!/bin/bash
mvn clean install  "-DpravegaVersion=0.8.0-2623.279ac21-SNAPSHOT" -DskipTests=true -Dlicense.skip=true
cd package/target/
tar xzf openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz
cd openmessaging-benchmark-0.0.1-SNAPSHOT
bin/benchmark --drivers driver-pravega/pravega-standalone.yaml workloads/schema-registry/1p-1c-rate-5k-user-avro.yaml 2>&1 | tee test1.log && grep Aggregated test1