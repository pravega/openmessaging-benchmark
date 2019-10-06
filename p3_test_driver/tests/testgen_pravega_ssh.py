#!/usr/bin/env python

from __future__ import print_function
import json
import sys


def add_test():
    driver = {
        'name': 'Pravega',
        'driverClass': 'io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver',
        'client': {
            'controllerURI': 'tcp://localhost:9090',
            'scopeName': 'examples',
        },
        'includeTimestampInEvent': includeTimestampInEvent,
    }
    workload = {
        'messageSize': messageSize,
        'topics':  topics,
        'partitionsPerTopic': partitionsPerTopic,
        'subscriptionsPerTopic': subscriptionsPerTopic,
        'consumerPerSubscription': consumerPerSubscription,
        'producersPerTopic': producersPerTopic,
        'producerRate': producerRateEventsPerSec,
        'consumerBacklogSizeGB': consumerBacklogSizeGB,
        'testDurationMinutes': testDurationMinutes,
        'keyDistributor': 'NO_KEY',
    }
    t = dict(
        test='openmessaging-benchmark',
        max_test_attempts=1,
        driver=driver,
        workload=workload,
        numWorkers=numWorkers,
        localWorker=localWorker,
        tarball=tarball,
        build=build,
        undeploy=True,
    )
    test_list.append(t)


test_list = []

localWorker = False
tarball = 'package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz'
build = False

# Message size 100 B
for repeat in range(1):
    for producerWorkers in [2]:
        numWorkers = 0 if localWorker else producerWorkers*2
        for testDurationMinutes in [15]:
            for messageSize in [100]:
                for producerRateEventsPerSec in [3e1, 1e2, 3e2, 1e3, 3e3, 1e4, 3e4, 1e5, 3e5, -1]:
                    for topics in [1]:
                        for partitionsPerTopic in [16]:
                            for producersPerWorker in [16]:
                                producersPerTopic = int(producersPerWorker * producerWorkers)
                                for consumerBacklogSizeGB in [0]:
                                    for subscriptionsPerTopic in [1]:
                                        for consumerPerSubscription in [partitionsPerTopic]:
                                            for includeTimestampInEvent in [True]:
                                                add_test()

# Message size 10 KB
for repeat in range(1):
    for producerWorkers in [2]:
        numWorkers = 0 if localWorker else producerWorkers*2
        for testDurationMinutes in [15]:
            for messageSize in [10000]:
                for producerRateEventsPerSec in [3e1, 1e2, 3e2, 1e3, 3e3, 1e4, -1]:
                    for topics in [1]:
                        for partitionsPerTopic in [16]:
                            for producersPerWorker in [2]:
                                producersPerTopic = int(producersPerWorker * producerWorkers)
                                for consumerBacklogSizeGB in [0]:
                                    for subscriptionsPerTopic in [1]:
                                        for consumerPerSubscription in [partitionsPerTopic]:
                                            for includeTimestampInEvent in [True]:
                                                add_test()

# Message size 1 MB
for repeat in range(1):
    for producerWorkers in [2]:
        numWorkers = 0 if localWorker else producerWorkers*2
        for testDurationMinutes in [15]:
            for messageSize in [1000*1000]:
                for producerRateEventsPerSec in [1e0, 3e0, 1e1, 3e1, 1e2, 3e2, -1]:
                    for topics in [1]:
                        for partitionsPerTopic in [16]:
                            for producersPerWorker in [2]:
                                producersPerTopic = int(producersPerWorker * producerWorkers)
                                for consumerBacklogSizeGB in [0]:
                                    for subscriptionsPerTopic in [1]:
                                        for consumerPerSubscription in [partitionsPerTopic]:
                                            for includeTimestampInEvent in [True]:
                                                add_test()

# Message size 100 B, 50,000 events/sec
for repeat in range(1):
    for producerWorkers in [2]:
        numWorkers = 0 if localWorker else producerWorkers*2
        for testDurationMinutes in [15]:
            for messageSize in [100]:
                for producerRateEventsPerSec in [5e4]:
                    for topics in [1]:
                        for partitionsPerTopic in [16, 6, 1]:
                            for producersPerWorker in [16, 6, 1]:
                                producersPerTopic = int(producersPerWorker * producerWorkers)
                                for consumerBacklogSizeGB in [0]:
                                    for subscriptionsPerTopic in [1]:
                                        for consumerPerSubscription in [partitionsPerTopic]:
                                            for includeTimestampInEvent in [True]:
                                                add_test()

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
