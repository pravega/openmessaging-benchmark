#!/usr/bin/env python

from __future__ import print_function
import json
import sys

test_list = []

localWorker = False
namespace = 'examples'
dockerRepository = 'claudiofahey'
imageTag = 'dev'
image = '%s/openmessaging-benchmark:%s' % (dockerRepository, imageTag)
tarball = 'package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz'
build = True

for repeat in range(1):
    for producerWorkers in [1]:
        numWorkers = 0 if localWorker else producerWorkers*2
        for testDurationMinutes in [15]:
            for messageSize in [1000000]:
                messageSize = int(messageSize)
                eps = []
                MBps = []
                ppw = []
                if messageSize <= 100:
                    eps += [30, 100, 300, 1000, 3000, 10000, 30000, 50000, 75000, 100000, 140000, -1]
                    ppw = [16]
                elif messageSize <= 10000:
                    eps += [30, 100, 300, 1000, 3000, 5000, 7000, 9000, -1]
                    #eps += [300]
                    ppw = [16]
                else:
                    #eps += [1, 3, 10, 30, 50, 70, 90, -1]
                    eps += [50, 70]
                    #MBps = [50.0]
                    ppw = [4]
                eps += [x * 1e6 / messageSize for x in MBps]
                for producerRateEventsPerSec in eps:
                    for topics in [1]:
                        for partitionsPerTopic in [16]:
                            for producersPerWorker in ppw:
                                producersPerTopic = producersPerWorker * producerWorkers
                                for consumerBacklogSizeGB in [0]:
                                    for subscriptionsPerTopic in [1]:
                                        for consumerPerSubscription in [partitionsPerTopic]:
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
                                                    max_test_attempts=2,
                                                    result_filename='data/results/json/%(test)s_%(test_uuid)s.json',
                                                    driver=driver,
                                                    workload=workload,
                                                    numWorkers=numWorkers,
                                                    localWorker=localWorker,
                                                    namespace=namespace,
                                                    image=image,
                                                    tarball=tarball,
                                                    build=build,
                                                    undeploy=True,
                                                )
                                                test_list += [t]
                                                build = False


# test_list = test_list[0:2]

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
