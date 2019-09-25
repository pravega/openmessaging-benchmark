# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import datetime
import logging
import os
import random
import string
from contextlib import contextmanager

# P3 Libraries
import p3_plugin_manager
import p3_storage
from p3_metrics import MetricsCollector
from p3_util import regex_first_group, record_result, updated_dict
from system_command import popen_to_queue, system_command, ssh, time_duration_to_seconds
from p3_test import StorageTest
from p3_test_hadoop import HadoopTest
from hadoop_util import kill_all_yarn_jobs, get_completed_job_info

_default_configs = {
    'all': {
        "app_master_memory_MB": 1024,
        "data_size_MB": 1*1000,
        "dfs_replication": 3,
        "hadoop_authentication": "standard",
        "io_file_buffer_size": 4096,
        "java_opts_xmx_ratio": 0.75,
        "map_cores": 1,
        "map_max_attempts": 1,
        "map_memory_MB": 2048,
        "map_output_compress_codec": "",
        "collect_text_files_node_manager": [
            '/etc/rc.local',
            '/etc/redhat-release',
            '/etc/sysctl.conf',
            '/etc/hadoop/conf/core-site.xml',
            '/etc/hadoop/conf/hdfs-site.xml',
            '/etc/hadoop/conf/mapred-site.xml',
            '/etc/hadoop/conf/yarn-site.xml',
            ],
        "reduce_cores": 1,
        "reduce_max_attempts": 1,
        "reduce_memory_MB": 2048,
        "sort_factor": 100,
        "sort_MiB": 256,
        "storage_hadoop_uri": "",
        "terasort_output_replication": 1,
        "test_variant": "standard",
        },
    'teragen': {
        "map_tasks": 24,
        },
    'terasort': {
        "reduce_tasks": 24,
        },
    'teravalidate': {
        },
    'grep': {    
        },
    'read': {
        "buffer_size": 1048576,
        "map_tasks": 24,
        "reduce_tasks": 0,
        "sort_MiB": 1,
        },
    'write': {
        "buffer_size": 1048576,
        "map_tasks": 24,
        "reduce_tasks": 0,
        "sort_MiB": 1,    
        },
    }

class PluginInfo(p3_plugin_manager.IP3Plugin):
    def get_plugin_info(self):
        return [
            {
            'class_type': 'test', 
            'class_name': 'teragen',
            'class': TeragenTest,
            },
            {
            'class_type': 'test', 
            'class_name': 'terasort', 
            'class': TerasortTest,
            },
            {
            'class_type': 'test', 
            'class_name': 'teravalidate', 
            'class': TeravalidateTest,
            },
            {
            'class_type': 'test', 
            'class_name': 'grep', 
            'class': GrepTest,
            },
            {
            'class_type': 'test', 
            'class_name': 'write', 
            'class': TestDFSIO,
            },
            {
            'class_type': 'test', 
            'class_name': 'read', 
            'class': TestDFSIO,
            },
        ]

class MapReduceTest(HadoopTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(MapReduceTest, self).__init__(test_config, default_configs=default_configs)

    def run_mapred_job(self):
        config = self.test_config

        with self.metrics_collector_context():
            self.start_metrics()

            # Build environment for command.
            env = None
            hadoop_command_env = config.get('hadoop_command_env')
            if hadoop_command_env:
                env = dict(os.environ)
                env.update(hadoop_command_env)

            logging.info('*****************************************************************');
            logging.info(config['test_desc'])
            
            t0 = datetime.datetime.utcnow()

            exit_code, output, errors = system_command(config['hadoop_command'], print_command=True, print_output=True, 
                raise_on_error=False, shell=False, noop=config['noop'], env=env,
                timeout=config.get('command_timeout_sec',None))
        
            t1 = datetime.datetime.utcnow()
            td = t1 - t0

            config['utc_begin'] = t0.isoformat()
            config['utc_end'] = t1.isoformat()
            config['elapsed_sec'] = time_duration_to_seconds(td)
            config['error'] = (exit_code != 0)
            config['command_timed_out'] = (exit_code == -1)
            config['exit_code'] = exit_code
            config['output'] = output
            config['errors'] = errors

            config['bytes_read_hdfs'] = float(regex_first_group('Bytes Read=(.*)', errors, return_on_no_match='nan', search=True))
            config['bytes_written_hdfs'] = float(regex_first_group('Bytes Written=(.*)', errors, return_on_no_match='nan', search=True))
            config['hadoop_job_id'] = regex_first_group('Running job: (job_[0-9_]+)', errors, search=True)

            self.get_completed_job_info()

    def get_completed_job_info(self):
        config = self.test_config
        get_completed_job_info(config)

    def get_hadoop_parameters(self):
        c = self.test_config
        params = []
        if 'hadoop_parameters' in c:
            params.extend(c['hadoop_parameters'])
        if 'block_size_MiB' in c:
            params.append('-Ddfs.blocksize=%dM' % c['block_size_MiB'])
        params.append('-Dio.file.buffer.size=%d' % c['io_file_buffer_size'])
        params.append('-Dmapreduce.map.cpu.vcores=%d' % c['map_cores'])
        params.append('-Dmapreduce.map.java.opts=-Xmx%dm' % (c['map_memory_MB'] * c['java_opts_xmx_ratio']))
        params.append('-Dmapreduce.map.maxattempts=%d' % c['map_max_attempts'])
        params.append('-Dmapreduce.map.memory.mb=%d' % c['map_memory_MB'])
        params.append('-Dmapreduce.map.output.compress=%s' % ('true' if c['map_output_compress_codec'] else 'false'))
        params.append('-Dmapreduce.map.output.compress.codec=%s' % c['map_output_compress_codec'])
        params.append('-Dmapreduce.reduce.cpu.vcores=%d' % c['reduce_cores'])
        params.append('-Dmapreduce.reduce.java.opts=-Xmx%dm'  % (c['reduce_memory_MB'] * c['java_opts_xmx_ratio']))
        params.append('-Dmapreduce.reduce.maxattempts=%d' % c['reduce_max_attempts'])
        params.append('-Dmapreduce.reduce.memory.mb=%d' % c['reduce_memory_MB'])
        params.append('-Dmapreduce.task.io.sort.factor=%d' % c['sort_factor'])
        params.append('-Dmapreduce.task.io.sort.mb=%d' % c['sort_MiB'])
        params.append('-Dyarn.app.mapreduce.am.command-opts=-Xmx%dm'  % (c['app_master_memory_MB'] * c['java_opts_xmx_ratio']))
        params.append('-Dyarn.app.mapreduce.am.resource.mb=%d' % c['app_master_memory_MB'])
        if 'map_tasks' in c:
            params.append('-Dmapred.map.tasks=%d' % c['map_tasks'])
        if 'reduce_tasks' in c:
            params.append('-Dmapred.reduce.tasks=%d' % c['reduce_tasks'])
        if 'dfs_replication' in c:
            params.append('-Ddfs.replication=%d' % c['dfs_replication'])
        return params

class TeragenTest(MapReduceTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(TeragenTest, self).__init__(test_config, default_configs=default_configs)

    def run_test(self):
        config = self.test_config

        self.hadoop_authenticate()
        self.configure_environment()

        base_directory = config['base_directory'] % config
        data_size_MB = config['data_size_MB']

        output_directory = '%s/terasort-input' % base_directory
        self.delete_hadoop_directory(output_directory)
        if not 'jar' in config: 
            config['jar'] = config['examples_jar']

        generic_options = self.get_hadoop_parameters()
        options = []

        rec_size = 100
        recs = int(data_size_MB * 1000.0 * 1000.0 / rec_size)

        cmd = []
        cmd.extend(['hadoop', 'jar', config['jar'], 'teragen'])
        cmd.extend(generic_options)
        cmd.extend(options)
        cmd.extend([str(recs), output_directory])
        config['hadoop_command'] = cmd

        self.run_mapred_job()

        if not config['error']:
            config['data_size_MB'] = config['bytes_written_hdfs'] / 1000.0 / 1000.0
            config['total_io_rate_MB_per_sec'] = config['data_size_MB'] / config['elapsed_sec']
            config['io_rate_MB_per_sec_per_storage_node'] = config['total_io_rate_MB_per_sec'] / config.get('storage_num_nodes',0.0)
            logging.info('RESULT: io_rate_MB_per_sec_per_storage_node=%f' % config['io_rate_MB_per_sec_per_storage_node'])
        self.record_result()
        if config['error']:
            raise Exception('Hadoop job failed')

class TerasortTest(MapReduceTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(TerasortTest, self).__init__(test_config, default_configs=default_configs)

    def run_test(self):
        config = self.test_config

        self.hadoop_authenticate()
        self.configure_environment()

        base_directory = config['base_directory'] % config

        input_directory = '%s/terasort-input' % base_directory
        output_directory = '%s/terasort-output' % base_directory
        self.delete_hadoop_directory(output_directory)

        if not self.hadoop_file_exists('%s/_SUCCESS' % input_directory):
            raise Exception('Previous teragen job did not succeed')

        if not 'jar' in config: 
            config['jar'] = config['examples_jar']

        generic_options = self.get_hadoop_parameters()
        generic_options.append('-Dmapreduce.terasort.output.replication=%d' % config['terasort_output_replication'])
        options = []
        cmd = []
        cmd.extend(['hadoop', 'jar', config['jar'], 'terasort'])
        cmd.extend(generic_options)
        cmd.extend(options)
        cmd.extend([input_directory, output_directory])
        config['hadoop_command'] = cmd

        self.run_mapred_job()

        if not config['error']:
            config['data_size_MB'] = config['bytes_read_hdfs'] / 1000.0 / 1000.0
            config['total_io_rate_MB_per_sec'] = config['data_size_MB'] / config['elapsed_sec']
            config['io_rate_MB_per_sec_per_storage_node'] = config['total_io_rate_MB_per_sec'] / config.get('storage_num_nodes',0.0)
            logging.info('RESULT: io_rate_MB_per_sec_per_storage_node=%f' % config['io_rate_MB_per_sec_per_storage_node'])
        self.record_result()
        if config['error']:
            raise Exception('Hadoop job failed')

class TeravalidateTest(MapReduceTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(TeravalidateTest, self).__init__(test_config, default_configs=default_configs)

    def run_test(self):
        config = self.test_config

        self.hadoop_authenticate()
        self.configure_environment()

        base_directory = config['base_directory'] % config

        input_directory = '%s/terasort-output' % base_directory
        output_directory = '%s/terasort-report' % base_directory
        self.delete_hadoop_directory(output_directory)

        if not self.hadoop_file_exists('%s/_SUCCESS' % input_directory):
            raise Exception('Previous terasort job did not succeed')

        if not 'jar' in config: 
            config['jar'] = config['examples_jar']

        generic_options = self.get_hadoop_parameters()
        generic_options.append('-Dmapreduce.terasort.output.replication=%d' % config['terasort_output_replication'])
        options = []
        cmd = []
        cmd.extend(['hadoop', 'jar', config['jar'], 'teravalidate'])
        cmd.extend(generic_options)
        cmd.extend(options)
        cmd.extend([input_directory, output_directory])
        config['hadoop_command'] = cmd

        self.run_mapred_job()

        if not config['error']:
            config['data_size_MB'] = config['bytes_read_hdfs'] / 1000.0 / 1000.0
            config['total_io_rate_MB_per_sec'] = config['data_size_MB'] / config['elapsed_sec']
            config['io_rate_MB_per_sec_per_storage_node'] = config['total_io_rate_MB_per_sec'] / config.get('storage_num_nodes',0.0)
            logging.info('RESULT: io_rate_MB_per_sec_per_storage_node=%f' % config['io_rate_MB_per_sec_per_storage_node'])
        self.record_result()
        if config['error']:
            raise Exception('Hadoop job failed')

class GrepTest(MapReduceTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(GrepTest, self).__init__(test_config, default_configs=default_configs)

    def run_test(self):
        config = self.test_config

        self.hadoop_authenticate()
        self.configure_environment()

        if 'input_directory' in config:
            input_directory = config['input_directory'] % config
        else:
            base_directory = config['base_directory'] % config
            input_directory = '%s/terasort-input' % base_directory

        if 'output_directory' in config:
            output_directory = config['output_directory'] % config
        else:
            base_directory = config['base_directory'] % config
            output_directory = '%s/grep-output' % base_directory

        if not 'grep_regex' in config:
            config['grep_regex'] = ''.join(random.choice(string.ascii_lowercase) for _ in range(20))

        self.delete_hadoop_directory(output_directory)

        if not self.hadoop_file_exists('%s/_SUCCESS' % input_directory):
            raise Exception('Previous job did not succeed')

        if not 'jar' in config: 
            config['jar'] = config['examples_jar']

        generic_options = self.get_hadoop_parameters()
        options = []
        cmd = []
        cmd.extend(['hadoop', 'jar', config['jar'], 'grep'])
        cmd.extend(generic_options)
        cmd.extend(options)
        cmd.extend([input_directory, output_directory, config['grep_regex']])
        config['hadoop_command'] = cmd

        self.run_mapred_job()

        if not config['error']:
            config['data_size_MB'] = config['bytes_read_hdfs'] / 1000.0 / 1000.0
            config['total_io_rate_MB_per_sec'] = config['data_size_MB'] / config['elapsed_sec']
            config['io_rate_MB_per_sec_per_storage_node'] = config['total_io_rate_MB_per_sec'] / config.get('storage_num_nodes',0.0)
            logging.info('RESULT: io_rate_MB_per_sec_per_storage_node=%f' % config['io_rate_MB_per_sec_per_storage_node'])
        self.record_result()
        if config['error']:
            raise Exception('Hadoop job failed')

class TestDFSIO(MapReduceTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(TestDFSIO, self).__init__(test_config, default_configs=default_configs)

    def run_test(self):
        config = self.test_config

        self.hadoop_authenticate()
        self.configure_environment()

        base_directory = config['base_directory'] % config
        data_size_MB = config['data_size_MB']
        config['requested_data_size_MB'] = data_size_MB

        data_directory = '%s/TestDFSIO' % base_directory

        if not 'jar' in config: 
            config['jar'] = config['job_client_jar']

        generic_options = self.get_hadoop_parameters()
        generic_options.append('-Dtest.build.data=%s' % data_directory)

        options = []
        options.extend(['-nrFiles', '%s' % config['map_tasks']])
        options.extend(['-bufferSize', '%s' % config['buffer_size']])
        config['job_name'] = 'TestDFSIO,%(test)s,%(map_tasks)d,%(data_size_MB)dMB' % config
        file_size_MB = int(data_size_MB / config['map_tasks'])
        options.extend(['-size', '%dMB' % file_size_MB])
        options.append('-%s' % config['test'])

        generic_options.append('-Dmapreduce.job.name=%s' % config['job_name'])

        # TestDFSIO doesn't use the correct file system. We may need to set fs.defaultFS.
        # Note that this will break the subsequent fetch of job info.
        # viprfs://hwxecs1bucket1.ns1.Site1/benchmarks/TestDFSIO
        # default_fs = regex_first_group('(.*://.*/).*', base_directory)
        # if default_fs:
        #     generic_options.append('-Dfs.defaultFS=%s' % default_fs)
        #     original_default_fs = 'hdfs://hwxecs2-master-0.solarch.local:8020'
        #     generic_options.append('-Dmapreduce.jobhistory.done-dir=%s/mr-history/done' % original_default_fs)
        #     generic_options.append('-Dmapreduce.jobhistory.intermediate-done-dir=%s/mr-history/tmp' % original_default_fs)
        #     generic_options.append('-Dyarn.app.mapreduce.am.staging-dir=%s/user' % original_default_fs)            

        cmd = []
        cmd.extend(['hadoop', 'jar', config['jar'], 'TestDFSIO'])
        cmd.extend(generic_options)
        cmd.extend(options)
        config['hadoop_command'] = cmd

        self.run_mapred_job()

        if not config['error']:
            config['data_size_MB'] = float(regex_first_group('Total MBytes processed: (.*)', config['errors'], return_on_no_match='nan', search=True))
            config['total_io_rate_MB_per_sec'] = config['data_size_MB'] / config['elapsed_sec']
        self.record_result()
        if config['error']:
            raise Exception('Hadoop job failed')
