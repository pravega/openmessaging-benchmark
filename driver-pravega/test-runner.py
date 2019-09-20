#!/usr/bin/env python
"""
Automatically run multiple OpenMessaging Benchmark tests in Kubernetes.
"""

import subprocess
import yaml
import json
import tempfile
import os
import uuid
import datetime


def create_yaml_file(data, file_name):
    """Creates a YAML file on the driver host."""
    f = tempfile.NamedTemporaryFile(mode='w', delete=False)
    yaml.dump(data, f, default_flow_style=False)
    f.close()
    cmd = ['kubectl', 'cp', f.name, 'examples/examples-openmessaging-benchmarking-driver:' + file_name]
    subprocess.run(cmd, check=True)
    os.unlink(f.name)


def run_single_benchmark(driver, workload, dry_run=False):
    test_uuid = str(uuid.uuid4())
    params = {
        'test_uuid': test_uuid,
        'utc_begin': datetime.datetime.utcnow().isoformat(),
        'driver': driver,
        'workload': workload,
    }
    # Encode all parameters in workload name attribute so they get written to the results file.
    workload['name'] = json.dumps(params)
    print(yaml.dump(params, default_flow_style=False))

    driver_file_name = '/tmp/driver-' +test_uuid + '.yaml'
    workload_file_name = '/tmp/workload-' + test_uuid + '.yaml'
    payload_file_name = '/tmp/payload-' + test_uuid + '.data'

    workload['payloadFile'] = payload_file_name

    create_yaml_file(driver, driver_file_name)
    create_yaml_file(workload, workload_file_name)

    cmd = [
        'kubectl', 'exec', '-n', 'examples', 'examples-openmessaging-benchmarking-driver', '--',
        'bash', '-c',
        'rm -f /tmp/logs.tar.gz'
        ' && dd if=/dev/urandom of=' + payload_file_name + ' bs=' + str(workload['messageSize']) + ' count=1 status=none'
        ' && bin/benchmark --drivers ' + driver_file_name + ' --workers $WORKERS ' + workload_file_name +
        ' && tar -czvf /tmp/logs-' + test_uuid + '.tar.gz *' + test_uuid + '*.json'
        ' && rm -f ' + payload_file_name
    ]
    if dry_run:
        print(' '.join(cmd))
    else:
        subprocess.run(cmd, check=True)

        # Collect and extract logs
        cmd = [
            'kubectl', 'cp',
            'examples/examples-openmessaging-benchmarking-driver:/tmp/logs-' + test_uuid + '.tar.gz',
            'logs/logs-' + test_uuid + '.tar.gz',
        ]
        subprocess.run(cmd, check=True)
        cmd = [
            'tar', '-xzvf', 'logs/logs-' + test_uuid + '.tar.gz',
            '-C', 'logs',
        ]
        subprocess.run(cmd, check=True)


def main():
    producerWorkers = 3
    for testDurationMinutes in [5]:
        for producerRate in [10, 100, 1000, 10000]:
            for topics in [1]:
                for partitionsPerTopic in [1, 6, 16]:
                    for producersPerTopic in [producerWorkers]:
                        for consumerBacklogSizeGB in [0]:
                            for subscriptionsPerTopic in [0]:
                                for consumerPerSubscription in [1]:
                                    for messageSize in [100, 10000]:
                                        driver = {
                                            'name': 'Pravega',
                                            'driverClass': 'io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver',
                                            'controllerURI': 'tcp://nautilus-pravega-controller.nautilus-pravega.svc.cluster.local:9090',
                                        }
                                        workload = {
                                            'topics':  topics,
                                            'partitionsPerTopic': partitionsPerTopic,
                                            'messageSize': messageSize,
                                            'subscriptionsPerTopic': subscriptionsPerTopic,
                                            'consumerPerSubscription': consumerPerSubscription,
                                            'producersPerTopic': producersPerTopic,
                                            'producerRate': producerRate,
                                            'consumerBacklogSizeGB': consumerBacklogSizeGB,
                                            'testDurationMinutes': testDurationMinutes,
                                        }
                                        run_single_benchmark(driver, workload, dry_run=False)


if __name__ == '__main__':
    main()
