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


def main():
    localWorker = False
    producerWorkers = 1
    namespace = 'examples'
    dockerRepository = 'claudiofahey'
    imageTag = 'dev'
    image = '%s/openmessaging-benchmark:%s' % (dockerRepository, imageTag)
    tarball = 'package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz'
    numWorkers = 0 if localWorker else producerWorkers*2

    deploy(numWorkers=numWorkers, image=image, namespace=namespace, tarball=tarball)

    for repeat in range(1):
        for testDurationMinutes in [5]:
            for producerRate in [1e9]: # 10, 100, 1000, 10000
                for topics in [1]:
                    for partitionsPerTopic in [96]:
                        for producersPerTopic in [producerWorkers*16]:
                            for consumerBacklogSizeGB in [0]:
                                for subscriptionsPerTopic in [0]:
                                    for consumerPerSubscription in [1]:
                                        for messageSize in [100]: #10000
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
                                                'keyDistributor': 'NO_KEY',

                                            }
                                            run_single_benchmark(driver, workload, dry_run=False, localWorker=localWorker, namespace=namespace)


def deploy(numWorkers, image, namespace, tarball, build=False):
    cmd = ['helm', 'delete', '--purge', '%s-openmessaging-benchmarking' % namespace]
    subprocess.run(cmd, check=False)
    if build:
        cmd = ['mvn', 'install']
        subprocess.run(cmd, check=True, cwd='..')
        cmd = [
            'docker', 'build',
            '--build-arg', 'BENCHMARK_TARBALL=%s' % tarball,
            '-f', '../docker/Dockerfile',
            '-t', image,
            '..',
        ]
        subprocess.run(cmd, check=True)
        cmd = ['docker', 'push', image]
        subprocess.run(cmd, check=True)
    cmd = [
        'kubectl', 'wait', '--for=delete', '--timeout=300s',
        '-n', namespace,
        'statefulset/%s-openmessaging-benchmarking-worker' % namespace,
        ]
    subprocess.run(cmd, check=False)
    cmd = [
        'kubectl', 'wait', '--for=delete', '--timeout=300s',
        '-n', namespace,
        'pod/%s-openmessaging-benchmarking-driver' % namespace,
        ]
    subprocess.run(cmd, check=False)
    cmd = [
        'helm', 'upgrade', '--install', '--timeout', '600', '--wait', '--debug',
        '%s-openmessaging-benchmarking' % namespace,
        '--namespace', namespace,
        '--set', 'image=%s' % image,
        '--set', 'numWorkers=%d' % numWorkers,
        '../deployment/kubernetes/helm/benchmark',
        ]
    subprocess.run(cmd, check=True)


def run_single_benchmark(driver, workload, namespace, dry_run=False, localWorker=False):
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

    create_yaml_file(driver, driver_file_name, namespace)
    create_yaml_file(workload, workload_file_name, namespace)

    if localWorker:
        workers_args = ''
    else:
        workers_args = '--workers $WORKERS'

    cmd = [
        'kubectl', 'exec', '-n', namespace, 'examples-openmessaging-benchmarking-driver', '--',
        'bash', '-c',
        'rm -f /tmp/logs.tar.gz'
        ' && dd if=/dev/urandom of=' + payload_file_name + ' bs=' + str(workload['messageSize']) + ' count=1 status=none'
        ' && bin/benchmark --drivers ' + driver_file_name + ' ' + workers_args + ' ' + workload_file_name +
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
            '%s/%s-openmessaging-benchmarking-driver:/tmp/logs-%s.tar.gz' % (namespace, namespace, test_uuid),
            'logs/logs-%s.tar.gz' % test_uuid,
            ]
        subprocess.run(cmd, check=True)
        cmd = [
            'tar', '-xzvf', 'logs/logs-%s.tar.gz' % test_uuid,
            '-C', 'logs',
                            ]
        subprocess.run(cmd, check=True)


def create_yaml_file(data, file_name, namespace):
    """Creates a YAML file on the driver host."""
    f = tempfile.NamedTemporaryFile(mode='w', delete=False)
    yaml.dump(data, f, default_flow_style=False)
    f.close()
    cmd = ['kubectl', 'cp', f.name, '%s/%s-openmessaging-benchmarking-driver:%s' % (namespace, namespace, file_name)]
    subprocess.run(cmd, check=True)
    os.unlink(f.name)


if __name__ == '__main__':
    main()
