
# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division

import datetime
import json
import logging
import os
import p3_plugin_manager
import subprocess
import tempfile
import yaml
from io import StringIO
from p3_test import TimeoutException, BaseTest
from p3_util import record_result
from system_command import system_command, time_duration_to_seconds, ssh

_default_configs = {
    'openmessaging-benchmark-k8s': {
        'print_output': True,
        'undeploy': False,
        'build': False,
        'noop': False,
    },
    'openmessaging-benchmark': {
        'print_output': True,
        'undeploy': False,
        'build': False,
        'noop': False,
        'terraform': False,
    },
}


class PluginInfo(p3_plugin_manager.IP3Plugin):
    def get_plugin_info(self):
        return [
            {
                'class_type': 'test',
                'class_name': 'openmessaging-benchmark-k8s',    # TODO: allow same class name as below
                'class': OpenMessagingBenchmarkK8sTest,
            },
            {
                'class_type': 'test',
                'class_name': 'openmessaging-benchmark',
                'class': OpenMessagingBenchmarkSSHTest,
            },
        ]


class OpenMessagingBenchmarkK8sTest(BaseTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(OpenMessagingBenchmarkK8sTest, self).__init__(test_config, default_configs=default_configs)

    def build(self):
        image = self.test_config['image']
        tarball = self.test_config['tarball']
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
        if self.test_config['build']:
            self.undeploy(wait=False)
            self.build()
        if self.test_config['build'] or self.test_config['undeploy']:
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
        cmd = [
            'kubectl', 'wait', '--for=condition=Ready', '--timeout=60s',
            '-n', namespace,
            'pod/%s-openmessaging-benchmarking-driver' % namespace,
            ]
        subprocess.run(cmd, check=True)
        for worker_number in range(numWorkers):
            cmd = [
                'kubectl', 'wait', '--for=condition=Ready', '--timeout=60s',
                '-n', namespace,
                'pod/%s-openmessaging-benchmarking-worker-%d' % (namespace, worker_number),
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
            workers = ['http://%s-openmessaging-benchmarking-worker-%d.%s-openmessaging-benchmarking-worker:8080' %
             (namespace, worker_number, namespace) for worker_number in range(numWorkers)]
            workers_args = '--workers %s' % ','.join(workers)

        cmd = [
            'kubectl', 'exec', '-n', namespace, 'examples-openmessaging-benchmarking-driver', '--',
            'bash', '-c',
            'rm -f /tmp/logs.tar.gz' +
            ' && dd if=/dev/urandom of=' + payload_file_name + ' bs=' + str(workload['messageSize']) + ' count=1 status=none' +
            ' && bin/benchmark --drivers ' + driver_file_name + ' ' + workers_args + ' ' + workload_file_name +
            ' && tar -czvf /tmp/logs-' + test_uuid + '.tar.gz *' + test_uuid + '*.json' +
            ' && rm -f ' + payload_file_name
        ]
        rec['_status_node'].set_status('Running command: %s' % str(cmd))

        t0 = datetime.datetime.utcnow()

        return_code, output, errors = system_command(
            cmd,
            print_output=True,
            shell=False,
            timeout=(workload['testDurationMinutes'] + 5) * 60,
            raise_on_error=False,
            noop=rec['noop'],
        )

        t1 = datetime.datetime.utcnow()
        td = t1 - t0

        logging.info('exit_code=%d' % return_code)

        rec['utc_begin'] = t0.isoformat()
        rec['utc_end'] = t1.isoformat()
        rec['elapsed_sec'] = time_duration_to_seconds(td)
        rec['error'] = (return_code != 0)
        rec['exit_code'] = return_code
        rec['command_timed_out'] = (return_code == -1)
        rec['output'] = output
        rec['errors'] = errors

        # Collect logs to store in results.json
        cmd = [
            'kubectl', 'exec', '-n', namespace, 'examples-openmessaging-benchmarking-driver', '--',
            'bash', '-c',
            'cat *' + test_uuid + '*.json',
        ]
        return_code, results_json, errors = system_command(cmd, print_output=False, shell=False, raise_on_error=False)
        rec['omb_results'] = json.load(StringIO(results_json.decode()))

        # Collect and extract logs (outside of results.json) (not required)
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

        rec['run_as_test'] = rec['test']
        if 'record_as_test' in rec:
            rec['test'] = rec['record_as_test']
        if 'result_filename' in rec:
            record_result(rec, rec['result_filename'])
        if rec['command_timed_out']:
            raise TimeoutException()
        if rec['error']:
            raise Exception('Command failed')


def create_yaml_file(data, file_name, namespace):
    """Creates a YAML file on the driver host."""
    f = tempfile.NamedTemporaryFile(mode='w', delete=False)
    yaml.dump(data, f, default_flow_style=False)
    f.close()
    cmd = ['kubectl', 'cp', f.name, '%s/%s-openmessaging-benchmarking-driver:%s' % (namespace, namespace, file_name)]
    subprocess.run(cmd, check=True)
    os.unlink(f.name)


class OpenMessagingBenchmarkSSHTest(BaseTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(OpenMessagingBenchmarkSSHTest, self).__init__(test_config, default_configs=default_configs)

    def build(self):
        # tarball = self.test_config['tarball']
        # cmd = ['mvn', 'install']
        # subprocess.run(cmd, check=True, cwd='..')
        pass

    def undeploy(self, wait=True):
        pass

    def deploy(self):
        pass

    def inspect_environment(self):
        rec = self.test_config
        driver = rec['driver']

        rec['git_commit'] = subprocess.run(['git', 'log', '--oneline', '-1'], capture_output=True, check=True).stdout.decode().rstrip()

        if rec['terraform']:
            driver_deploy_dir = '../driver-%s/deploy' % driver['name'].lower()
            terraform_show_json = subprocess.run(
                ['terraform', 'show', '-json'],
                cwd=driver_deploy_dir,
                check=True,
                capture_output=True,
            ).stdout.decode()
            rec['terraform_show'] = json.load(StringIO(terraform_show_json))
            logging.info('terraform_show=%s' % rec['terraform_show'])

            if rec.get('ssh_host', '') == '':
                rec['ssh_host'] = subprocess.run(
                    ['terraform', 'output', 'client_ssh_host'],
                    cwd=driver_deploy_dir,
                    check=True,
                    capture_output=True,
                ).stdout.decode().rstrip()
                logging.info('ssh_host=%s' % rec['ssh_host'])

        if rec['ansible']:
            with open('%s/vars.yaml' % driver_deploy_dir) as vars_file:
                rec['ansible_vars'] = yaml.load(vars_file)
            logging.info("ansible_vars=%s" % str(rec['ansible_vars']))

    def run_test(self):
        rec = self.test_config
        test_uuid = rec['test_uuid']
        driver = rec['driver']
        workload = rec['workload']
        numWorkers = rec['numWorkers']
        localWorker = rec['localWorker']

        self.inspect_environment()

        workload['name'] = test_uuid
        driver_file_name = '/tmp/driver-' +test_uuid + '.yaml'
        workload_file_name = '/tmp/workload-' + test_uuid + '.yaml'
        payload_file_name = '/tmp/payload-' + test_uuid + '.data'
        workload['payloadFile'] = payload_file_name

        self.deploy()

        if localWorker:
            # TODO: Doesn't work because workers.yaml exists.
            workers_args = ''
        else:
            return_code, results_yaml, errors = self.ssh('cat /opt/benchmark/workers.yaml')
            workers = yaml.load(StringIO(results_yaml))['workers']
            workers = workers[0:numWorkers]
            logging.info("workers=%s" % str(workers))
            rec['omb_workers'] = workers
            workers_args = '--workers %s' % ','.join(workers)

        if driver['name'] == 'Pravega':
            return_code, results_yaml, errors = self.ssh('cat /opt/benchmark/driver-pravega/pravega.yaml')
            deployed_driver = yaml.load(StringIO(results_yaml))
            driver['client']['controllerURI'] = deployed_driver['client']['controllerURI']
        elif driver['name'] == 'Pulsar':
            return_code, results_yaml, errors = self.ssh('cat /opt/benchmark/driver-pulsar/pulsar.yaml')
            deployed_driver = yaml.load(StringIO(results_yaml))
            driver['client']['serviceUrl'] = deployed_driver['client']['serviceUrl']
            driver['client']['httpUrl'] = deployed_driver['client']['httpUrl']
        else:
            raise Exception('Unsupported driver')

        self.create_yaml_file(driver, driver_file_name)
        self.create_yaml_file(workload, workload_file_name)

        cmd = (
            'cd /opt/benchmark' +
            ' && sudo chmod go+rw .' +
            ' && dd if=/dev/urandom of=' + payload_file_name + ' bs=' + str(workload['messageSize']) + ' count=1 status=none' +
            ' && bin/benchmark --drivers ' + driver_file_name + ' ' + workers_args + ' ' + workload_file_name
        )
        rec['_status_node'].set_status('Running command: %s' % str(cmd))

        t0 = datetime.datetime.utcnow()

        return_code, output, errors = self.ssh(cmd, raise_on_error=False)

        t1 = datetime.datetime.utcnow()
        td = t1 - t0

        rec['utc_begin'] = t0.isoformat()
        rec['utc_end'] = t1.isoformat()
        rec['elapsed_sec'] = time_duration_to_seconds(td)
        rec['error'] = (return_code != 0)
        rec['exit_code'] = return_code
        rec['command_timed_out'] = (return_code == -1)
        rec['output'] = output
        rec['errors'] = errors

        # Collect results to store in results.json
        try:
            return_code, results_json, errors = self.ssh(
                'cat /opt/benchmark/*' + test_uuid + '*.json',
                print_output=False,
            )
            rec['omb_results'] = json.load(StringIO(results_json.decode()))
        except Exception as e:
            logging.warn('Unable to collect logs: %s' % e)
            rec['error'] = True

        rec['run_as_test'] = rec['test']
        if 'record_as_test' in rec:
            rec['test'] = rec['record_as_test']
        if 'result_filename' in rec:
            record_result(rec, rec['result_filename'])
        if rec['command_timed_out']:
            raise TimeoutException()
        if rec['error']:
            raise Exception('Command failed')

    def create_yaml_file(self, data, file_name):
        """Creates a YAML file on the driver host."""
        f = tempfile.NamedTemporaryFile(mode='w', delete=False)
        yaml.dump(data, f, default_flow_style=False)
        f.close()
        cmd = [
            'scp',
            '-i', self.test_config['ssh_identity_file'],
            f.name,
            '%s@%s:%s' % (self.test_config['ssh_user'], self.test_config['ssh_host'], file_name),
        ]
        subprocess.run(cmd, check=True)
        os.unlink(f.name)

    def ssh(self, command, raise_on_error=True, stderr_to_stdout=False, print_output=True, **kwargs):
        return ssh(
            self.test_config['ssh_user'],
            self.test_config['ssh_host'],
            command,
            opts='-i ' + self.test_config['ssh_identity_file'],
            print_output=print_output,
            raise_on_error=raise_on_error,
            stderr_to_stdout=stderr_to_stdout,
            **kwargs
        )
