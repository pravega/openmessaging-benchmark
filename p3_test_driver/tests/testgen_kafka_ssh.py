#!/usr/bin/env python

from __future__ import print_function
import json
import sys
import ruamel.yaml

def add_test():
    driver = {
        'name': 'Kafka',
        'driverClass': 'io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver',
        'replicationFactor': 3,

        'commonConfig':
          "bootstrap.servers=127.0.0.1",

        'topicConfig':
          "min.insync.replicas=3\n"
          # This is for syncing to disk
          # "flush.messages=1\n"
          # "flush.ms=0\n",

        'producerConfig':
          "acks=all\n"
          "linger.ms=1\n"
          "batch.size=131072\n"
          # This is for transaction
          # "enableTransaction=True\n"
          # "eventPerTransaction=100\n"
          # "enable.idempotence=true",

        'consumerConfig':
          "auto.offset.reset=earliest\n"
          "enable.auto.commit=false",
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
    for producerWorkers in [1]:
        numWorkers = 0 if localWorker else producerWorkers*2
        for testDurationMinutes in [5]:
            for messageSize in [100]:
                for producerRateEventsPerSec in [1e2, 1e3, 5e3, 1e4, 5e4, 1e5, 5e5, -1]:
                    for topics in [1]:
                        for partitionsPerTopic in [1]:
                            for producersPerWorker in [1]:
                                producersPerTopic = int(producersPerWorker * producerWorkers)
                                for consumerBacklogSizeGB in [0]:
                                    for subscriptionsPerTopic in [1]:
                                        for consumerPerSubscription in [partitionsPerTopic]:
                                            for includeTimestampInEvent in [True]:
                                                add_test()

# Message size 10 KB
for repeat in range(1):
    for producerWorkers in [1]:
        numWorkers = 0 if localWorker else producerWorkers*2
        for testDurationMinutes in [5]:
            for messageSize in [10000]:
                for producerRateEventsPerSec in [500, 1000, 3000, 6000, 9000, 12000, 15000, -1]:
                    for topics in [1]:
                        for partitionsPerTopic in [1]:
                            for producersPerWorker in [1]:
                                producersPerTopic = int(producersPerWorker * producerWorkers)
                                for consumerBacklogSizeGB in [0]:
                                    for subscriptionsPerTopic in [1]:
                                        for consumerPerSubscription in [partitionsPerTopic]:
                                            for includeTimestampInEvent in [True]:
                                                add_test()
#
# Message size 100 KB
for repeat in range(1):
    for producerWorkers in [1]:
        numWorkers = 0 if localWorker else producerWorkers*2
        for testDurationMinutes in [5]:
            for messageSize in [100000]:
                for producerRateEventsPerSec in [10, 50, 100, 300, 600, 900, 1200, -1]:
                # for producerRateEventsPerSec in [1300, 1500, 1700]:
                    for topics in [1]:
                        for partitionsPerTopic in [1]:
                            for producersPerWorker in [1]:
                                producersPerTopic = int(producersPerWorker * producerWorkers)
                                for consumerBacklogSizeGB in [0]:
                                    for subscriptionsPerTopic in [1]:
                                        for consumerPerSubscription in [partitionsPerTopic]:
                                            for includeTimestampInEvent in [True]:
                                                add_test()
# # #
# # # # Message size 1 MB
for repeat in range(1):
    for producerWorkers in [1]:
        numWorkers = 0 if localWorker else producerWorkers*2
        for testDurationMinutes in [5]:
            for messageSize in [1000000]:
                for producerRateEventsPerSec in [1, 10, 30, 50, 70, 90, 110, 130, -1]:
                # for producerRateEventsPerSec in [1]:
                    for topics in [1]:
                        for partitionsPerTopic in [1]:
                            for producersPerWorker in [1]:
                                producersPerTopic = int(producersPerWorker * producerWorkers)
                                for consumerBacklogSizeGB in [0]:
                                    for subscriptionsPerTopic in [1]:
                                        for consumerPerSubscription in [partitionsPerTopic]:
                                            for includeTimestampInEvent in [True]:
                                                add_test()

# # Message size 100 B, 50,000 events/sec
# for repeat in range(1):
#     for producerWorkers in [1]:
#         numWorkers = 0 if localWorker else producerWorkers*2
#         for testDurationMinutes in [5]:
#             for messageSize in [100]:
#                 for producerRateEventsPerSec in [5e4]:
#                     for topics in [1]:
#                         for partitionsPerTopic in [16, 6, 1]:
#                             for producersPerWorker in [1]:
#                                 producersPerTopic = int(producersPerWorker * producerWorkers)
#                                 for consumerBacklogSizeGB in [0]:
#                                     for subscriptionsPerTopic in [1]:
#                                         for consumerPerSubscription in [partitionsPerTopic]:
#                                             for includeTimestampInEvent in [True]:
#                                                 add_test()

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
