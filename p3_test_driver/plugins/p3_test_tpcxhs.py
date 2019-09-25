# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import datetime
import logging
import os
import time
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
    'tpcxhs': {
        "app_master_memory_MB": 1024,
        "base_directory": "/benchmarks",
        "data_size_MB": 1*1000,
        "hadoop_authentication": "standard",
        "io_file_buffer_size": 4096,
        "jar": "TPCx-HS-master.jar",
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
        "sleep_after_delete_sec": 5,
        "sort_factor": 100,
        "sort_MiB": 256,
        },
    }

class PluginInfo(p3_plugin_manager.IP3Plugin):
    def get_plugin_info(self):
        return [
            {
            'class_type': 'test', 
            'class_name': 'tpcxhs',
            'class': TpcxHsTest,
            },
        ]

class TpcxHsTest(HadoopTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(TpcxHsTest, self).__init__(test_config, default_configs=default_configs)

    def run_test(self):
        rec = self.test_config

        data_size_MB = rec['data_size_MB']
        base_directory = rec['base_directory'] % rec
        test_directory = '%s/TPCx-HS-benchmark' % base_directory
        sort_input_directory = '%s/HSsort-input' % test_directory
        sort_output_directory = '%s/HSsort-output' % test_directory
        validate_output_directory = '%s/HSValidate' % test_directory

        rec['data_size_TB'] = rec['data_size_MB'] / 1e6
        rec['sf'] = rec['data_size_TB']
        rec['error'] = False

        #
        # Build commands
        #

        # HSGen
        rec_size = 100
        recs = int(data_size_MB * 1000.0 * 1000.0 / rec_size)
        cmd = []
        cmd.extend(['hadoop', 'jar', rec['jar'], 'HSGen'])
        cmd.extend(get_hadoop_parameters(rec))
        cmd.extend([str(recs), sort_input_directory])
        rec['hsgen:hadoop_command'] = cmd

        # HSSort
        cmd = []
        cmd.extend(['hadoop', 'jar', rec['jar'], 'HSSort'])
        cmd.extend(get_hadoop_parameters(rec))
        cmd.extend([sort_input_directory, sort_output_directory])
        rec['hssort:hadoop_command'] = cmd

        # HSValidate
        cmd = []
        cmd.extend(['hadoop', 'jar', rec['jar'], 'HSValidate'])
        hsvalidate_config = rec.copy()
        del hsvalidate_config['map_tasks']
        del hsvalidate_config['reduce_tasks']
        cmd.extend(get_hadoop_parameters(hsvalidate_config))
        cmd.extend([sort_output_directory, validate_output_directory])
        rec['hsvalidate:hadoop_command'] = cmd

        for key in ['hsgen:hadoop_command','hssort:hadoop_command','hsvalidate:hadoop_command']:
            logging.info('%s: %s' % (key, rec[key]))

        #
        # Prepare for benchmark
        #

        self.hadoop_authenticate()
        self.configure_environment()
        self.delete_hadoop_directory('%s/*' % test_directory)
        system_command(['hadoop', 'fs', '-expunge'], print_command=True, print_output=True, raise_on_error=True, shell=False)
        logging.info('Sleeping for %0.0f seconds' % rec['sleep_after_delete_sec'])
        time.sleep(rec['sleep_after_delete_sec'])

        with self.metrics_collector_context():
            self.start_metrics()

            #
            # Run benchmark
            #

            t0 = datetime.datetime.utcnow()

            try:
                # HSGen
                self.run_mapred_job(key_prefix='hsgen:', raise_on_error=True)            
                system_command(['hdfs', 'dfs', '-ls', '%s/*' % sort_input_directory], print_command=True, print_output=True, raise_on_error=True, shell=False)

                # HSSort
                self.run_mapred_job(key_prefix='hssort:', raise_on_error=True)
                system_command(['hdfs', 'dfs', '-ls', '%s/*' % sort_output_directory], print_command=True, print_output=True, raise_on_error=True, shell=False)

                # HSValidate
                self.run_mapred_job(key_prefix='hsvalidate:', raise_on_error=True)
                system_command(['hdfs', 'dfs', '-ls', '%s/*' % validate_output_directory], print_command=True, print_output=True, raise_on_error=True, shell=False)
            except:
                logging.error('EXCEPTION: %s' % traceback.format_exc())
                rec['error'] = True

            t1 = datetime.datetime.utcnow()
            td = t1 - t0

        rec['elapsed_sec'] = time_duration_to_seconds(td)
        if not rec['error']:
            rec['total_io_rate_MB_per_sec'] = rec['data_size_MB'] / rec['elapsed_sec']
            rec['io_rate_MB_per_sec_per_storage_node'] = rec['total_io_rate_MB_per_sec'] / rec.get('storage_num_nodes',float('nan'))
            rec['HSph@SF'] = rec['sf'] / (rec['elapsed_sec'] / 3600.0)
            logging.info('RESULT: elapsed_sec=%f, HSph@SF=%f' % (rec['elapsed_sec'], rec['HSph@SF']))
        self.record_result()
        if rec['error']:
            raise Exception('Test failed')

    def run_mapred_job(self, key_prefix='', raise_on_error=False):
        rec = self.test_config

        # Build environment for command.
        env = None
        hadoop_command_env = rec.get('%shadoop_command_env' % key_prefix)
        if hadoop_command_env:
            env = dict(os.environ)
            env.update(hadoop_command_env)

        t0 = datetime.datetime.utcnow()

        return_code, output, errors = system_command(
            rec['%shadoop_command' % key_prefix], 
            print_command=True, 
            print_output=True, 
            timeout=rec.get('%scommand_timeout_sec' % key_prefix), 
            raise_on_error=False, 
            shell=False,
            noop=rec.get('%snoop' % key_prefix, False), 
            env=env)
    
        t1 = datetime.datetime.utcnow()
        td = t1 - t0

        rec['%sutc_begin' % key_prefix] = t0.isoformat()
        rec['%sutc_end' % key_prefix] = t1.isoformat()
        rec['%selapsed_sec' % key_prefix] = time_duration_to_seconds(td)
        rec['%serror' % key_prefix] = (return_code != 0)
        rec['%scommand_timed_out' % key_prefix] = (return_code == -1)
        rec['%sexit_code' % key_prefix] = return_code
        rec['%soutput' % key_prefix] = output
        rec['%serrors' % key_prefix] = errors
        rec['%sbytes_read_hdfs' % key_prefix] = float(regex_first_group('Bytes Read=(.*)', errors, return_on_no_match='nan', search=True))
        rec['%sbytes_written_hdfs' % key_prefix] = float(regex_first_group('Bytes Written=(.*)', errors, return_on_no_match='nan', search=True))
        rec['%shadoop_job_id' % key_prefix] = regex_first_group('Running job: (job_[0-9_]+)', errors, search=True)

        if rec['%serror' % key_prefix]:
            raise Exception('Hadoop job failed')

def get_hadoop_parameters(config):
    c = config
    params = []
    if 'hadoop_parameters' in c:
        params.extend(c['hadoop_parameters'])
    if 'block_size_MiB' in c:
        params.append('-Ddfs.blocksize=%dM' % c['block_size_MiB'])
    if 'io_file_buffer_size' in c:
        params.append('-Dio.file.buffer.size=%d' % c['io_file_buffer_size'])
    if 'map_cores' in c:
        params.append('-Dmapreduce.map.cpu.vcores=%d' % c['map_cores'])
    if 'map_max_attempts' in c:
        params.append('-Dmapreduce.map.maxattempts=%d' % c['map_max_attempts'])
    if 'map_memory_MB' in c:
        params.append('-Dmapreduce.map.memory.mb=%d' % c['map_memory_MB'])
        params.append('-Dmapreduce.map.java.opts=-Xmx%dm' % (c['map_memory_MB'] * c['java_opts_xmx_ratio']))
    if 'map_output_compress_codec' in c:
        params.append('-Dmapreduce.map.output.compress=%s' % ('true' if c['map_output_compress_codec'] else 'false'))
        params.append('-Dmapreduce.map.output.compress.codec=%s' % c['map_output_compress_codec'])
    if 'reduce_cores' in c:
        params.append('-Dmapreduce.reduce.cpu.vcores=%d' % c['reduce_cores'])
    if 'reduce_memory_MB' in c:
        params.append('-Dmapreduce.reduce.memory.mb=%d' % c['reduce_memory_MB'])
        params.append('-Dmapreduce.reduce.java.opts=-Xmx%dm'  % (c['reduce_memory_MB'] * c['java_opts_xmx_ratio']))
    if 'reduce_max_attempts' in c:
        params.append('-Dmapreduce.reduce.maxattempts=%d' % c['reduce_max_attempts'])
    if 'sort_factor' in c:
        params.append('-Dmapreduce.task.io.sort.factor=%d' % c['sort_factor'])
    if 'sort_MiB' in c:
        params.append('-Dmapreduce.task.io.sort.mb=%d' % c['sort_MiB'])
    if 'app_master_memory_MB' in c:
        params.append('-Dyarn.app.mapreduce.am.resource.mb=%d' % c['app_master_memory_MB'])
        params.append('-Dyarn.app.mapreduce.am.command-opts=-Xmx%dm'  % (c['app_master_memory_MB'] * c['java_opts_xmx_ratio']))
    if 'map_tasks' in c:
        params.append('-Dmapred.map.tasks=%d' % c['map_tasks'])
        params.append('-Dmapreduce.job.maps=%d' % c['map_tasks'])
    if 'reduce_tasks' in c:
        params.append('-Dmapred.reduce.tasks=%d' % c['reduce_tasks'])
        params.append('-Dmapreduce.job.reduces=%d' % c['reduce_tasks'])
    return params
