
# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import logging
import os
import sys
import datetime

# P3 Libraries
import p3_plugin_manager
import p3_storage
from p3_metrics import MetricsCollector
from p3_util import record_result
from system_command import system_command, time_duration_to_seconds
from hadoop_util import configure_compute
from p3_test import TimeoutException, StorageTest

_default_configs = {
    'simple': {
        'print_output': True,
        },
    'simple_hadoop': {
        "collect_text_files_node_manager": [
            '/etc/rc.local',
            '/etc/redhat-release',
            '/etc/sysctl.conf',
            '/etc/hadoop/conf/core-site.xml',
            '/etc/hadoop/conf/hdfs-site.xml',
            '/etc/hadoop/conf/mapred-site.xml',
            '/etc/hadoop/conf/yarn-site.xml',
            ],
        'print_output': True,
        }
    }

class PluginInfo(p3_plugin_manager.IP3Plugin):
    def get_plugin_info(self):
        return [
            {
            'class_type': 'test', 
            'class_name': 'simple',
            'class': SimpleTest,
            },
            {
            'class_type': 'test', 
            'class_name': 'simple_hadoop',
            'class': SimpleHadoopTest,
            },
        ]

class SimpleTest(StorageTest):
    """P3 generic test class that can be used to run tests consisting of a single command."""

    def __init__(self, test_config, default_configs=_default_configs):
        super(SimpleTest, self).__init__(test_config, default_configs=default_configs)

    def configure_environment(self):
        self.configure_storage()

    def run_test(self):
        rec = self.test_config

        self.configure_environment()

        # Build command from command template.
        if 'command' not in rec and 'command_template' in rec:
            if isinstance(rec['command_template'], list):
                rec['command'] = [x % rec for x in rec['command_template']]
            else:
                rec['command'] = rec['command_template'] % rec
        cmd = rec['command']

        if 'command_shell' not in rec:
            rec['command_shell'] = not isinstance(cmd, list)

        # Build environment for command.
        env = None
        command_env = rec.get('command_env')
        if command_env:
            env = dict(os.environ)
            env.update(command_env)

        rec['_status_node'].set_status('Running command: %s' % str(cmd))

        with self.metrics_collector_context():
            self.start_metrics()

            t0 = datetime.datetime.utcnow()
            
            return_code, output, errors = system_command(cmd, 
                print_command=True, 
                print_output=rec['print_output'], 
                timeout=rec.get('command_timeout_sec',None), 
                raise_on_error=False,
                shell=rec['command_shell'],
                noop=rec['noop'], 
                env=env)

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
            rec['run_as_test'] = rec['test']
            if 'record_as_test' in rec:
                rec['test'] = rec['record_as_test']

        if 'result_filename' in rec:
            record_result(rec, rec['result_filename'])

        if rec['command_timed_out']:
            raise TimeoutException()
        if rec['error']:
            raise Exception('Command failed')

class SimpleHadoopTest(SimpleTest):
    """Identical to SimpleTest except that it configures and records the Hadoop environment."""

    def __init__(self, test_config, default_configs=_default_configs):
        super(SimpleHadoopTest, self).__init__(test_config, default_configs=default_configs)

    def configure_environment(self):
        self.configure_storage()
        self.configure_compute()

    def configure_compute(self):
        config = self.test_config
        configure_compute(config)
