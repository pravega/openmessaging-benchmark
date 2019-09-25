
# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import logging
import os
import sys
import datetime
import subprocess
import yaml
import json
import tempfile

# P3 Libraries
import p3_plugin_manager
import p3_storage
from p3_metrics import MetricsCollector
from p3_util import record_result
from system_command import system_command, time_duration_to_seconds
from p3_test import TimeoutException, StorageTest, BaseTest

_default_configs = {
    'openmessaging-benchmark': {
        'print_output': True,
        },
    }


class PluginInfo(p3_plugin_manager.IP3Plugin):
    def get_plugin_info(self):
        return [
            {
            'class_type': 'test', 
            'class_name': 'openmessaging-benchmark',
            'class': OpenMessagingBenchmarkTest,
            },
        ]


class OpenMessagingBenchmarkTest(BaseTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(OpenMessagingBenchmarkTest, self).__init__(test_config, default_configs=default_configs)

    def undeploy(self, wait=True):
        namespace = self.test_config['namespace']
        cmd = ['helm', 'delete', '--purge', '%s-openmessaging-benchmarking' % namespace]
        subprocess.run(cmd, check=False)
        if wait:
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


    def deploy(self):
        numWorkers = self.test_config['numWorkers']
        image = self.test_config['image']
        namespace = self.test_config['namespace']
        self.undeploy(wait=True)
        cmd = [
            'helm', 'upgrade', '--install', '--timeout', '600', '--wait', '--debug',
            '%s-openmessaging-benchmarking' % namespace,
            '--namespace', namespace,
            '--set', 'image=%s' % image,
            '--set', 'numWorkers=%d' % numWorkers,
            '../deployment/kubernetes/helm/benchmark',
            ]
        subprocess.run(cmd, check=True)


    def run_test(self):
        rec = self.test_config

        self.deploy()

        git_commit = subprocess.run(['git', 'log', '--oneline', '-1'], capture_output=True, check=True).stdout.decode()

        test_uuid = rec['test_uuid']
        driver = rec['driver']
        workload = rec['workload']
        numWorkers = rec['numWorkers']
        localWorker = rec['localWorker']
        namespace = rec['namespace']
        dry_run = rec['dry_run']

        params = {
            'test_uuid': test_uuid,
            'utc_begin': rec['utc_begin'],
            'driver': driver,
            'workload': workload,
            'numWorkers': numWorkers,
            'git_commit': git_commit,
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
            'rm -f /tmp/logs.tar.gz' +
            ' && dd if=/dev/urandom of=' + payload_file_name + ' bs=' + str(workload['messageSize']) + ' count=1 status=none' +
            ' && bin/benchmark --drivers ' + driver_file_name + ' ' + workers_args + ' ' + workload_file_name +
            ' && tar -czvf /tmp/logs-' + test_uuid + '.tar.gz *' + test_uuid + '*.json' +
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
