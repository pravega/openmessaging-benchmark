#!/usr/bin/env python

from __future__ import print_function
import json
import sys

test_list = []

localWorker = True
namespace = 'examples'
dockerRepository = 'claudiofahey'
imageTag = 'dev'
image = '%s/openmessaging-benchmark:%s' % (dockerRepository, imageTag)
tarball = 'package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz'

for repeat in range(3):
    for producerWorkers in [1]:
        numWorkers = 0 if localWorker else producerWorkers*2
        for testDurationMinutes in [5]:
            for messageSize in [1e2]:
                messageSize = int(messageSize)
                eps = [-1]
                MBps = []
                if messageSize <= 1e2:
                    eps += [50000]
                else:
                    MBps = [50.0]
                eps += [x * 1e6 / messageSize for x in MBps]
                for producerRateEventsPerSec in [0]:
                    for topics in [1]:
                        for partitionsPerTopic in [1]:
                            for producersPerWorker in [1]:
                                producersPerTopic = producersPerWorker * producerWorkers
                                for consumerBacklogSizeGB in [0]:
                                    for subscriptionsPerTopic in [1]:
                                        for consumerPerSubscription in [producersPerTopic]:
                                            for includeTimestampInEvent in [True]:
                                                driver = {
                                                    'name': 'Pravega',
                                                    'driverClass': 'io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver',
                                                    'client': {
                                                        'controllerURI': 'tcp://nautilus-pravega-controller.nautilus-pravega.svc.cluster.local:9090',
                                                        'scopeName': 'examples',
                                                    },
                                                    'includeTimestampInEvent': includeTimestampInEvent,
                                                }
                                                workload = {
                                                    'topics':  topics,
                                                    'partitionsPerTopic': partitionsPerTopic,
                                                    'messageSize': messageSize,
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
                                                    dry_run=False,
                                                    numWorkers=numWorkers,
                                                    localWorker=localWorker,
                                                    namespace=namespace,
                                                    image=image,
                                                    tarball=tarball,
                                                    build=True,
                                                )
                                                test_list += [t]


# test_list = test_list[0:2]

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
