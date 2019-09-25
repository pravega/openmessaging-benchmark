
# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import datetime
import logging
import os
import subprocess
import sys
import multiprocessing
import shutil
import glob
import uuid
import re
import Queue
import random
import copy
from contextlib import contextmanager

# P3 Libraries
import p3_plugin_manager
import p3_storage
from p3_metrics import MetricsCollector
from p3_util import regex_first_group, regex_groups, record_result, updated_dict, read_file_to_string, glob_file_list
from system_command import popen_to_queue, system_command, ssh, time_duration_to_seconds
from p3_test_hadoop import HadoopTest
from hadoop_util import kill_all_yarn_jobs, kill_yarn_job
from p3_test import TimeoutException

_default_configs = {
    'sqlbatch': {
        'queries_per_stream': 0,
        'random_seed': 0,
        'stream_count': 1,
        }
    }

class PluginInfo(p3_plugin_manager.IP3Plugin):
    def get_plugin_info(self):
        return [
            {
            'class_type': 'test', 
            'class_name': 'sqlquery',
            'class': SqlQueryTest,
            },
            {
            'class_type': 'test', 
            'class_name': 'sqlbatch',
            'class': SqlBatchTest,
            },
        ]

class SqlTest(HadoopTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(SqlTest, self).__init__(test_config, default_configs=default_configs)

    def configure_environment(self):
        config = self.test_config
        super(SqlTest, self).configure_environment()

        db_type = config['db_type']
        
        if db_type == 'hawq':
            if config.get('restart_hawq',False):
                system_command('/etc/init.d/hawq stop')
                system_command('/etc/init.d/hawq start')

        elif db_type == 'impala':
            cmd = []
            cmd.extend(['impala-shell'])
            cmd.extend(['--impalad', '%s:%d' % (config.get('impalad_host','localhost'), config.get('impalad_port',21000))])
            cmd.extend(['--database', self.db_name()])
            cmd.extend(['-q', 'invalidate metadata'])
            system_command(cmd, 
                print_command=True, 
                print_output=True, 
                raise_on_error=True,
                shell=False)

    def db_name(self):
        config = self.test_config
        return config['db_name'] % config

class SqlQueryTest(SqlTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(SqlQueryTest, self).__init__(test_config, default_configs=default_configs)

    def run_test(self):
        config = self.test_config
        
        # If run in the optimizer, we don't want to run much longer than the best sample.
        if config.get('optimizer_set_timeout') and 'optimizer_best_query_elapsed_sec_mean' in config and 'optimizer_best_query_elapsed_sec_std' in config:
            max_query_elapsed_sec = config['optimizer_best_query_elapsed_sec_mean'] + 3*config['optimizer_best_query_elapsed_sec_std']
            config['command_timeout_sec'] = 30.0 + max_query_elapsed_sec
            logging.info('SqlQueryTest.run_test: Setting command timeout to %0.0f seconds' % config['command_timeout_sec'])

        config['_status_node'].set_status('Running query %s' % config['query_filename'])

        self.hadoop_authenticate()
        self.configure_environment()

        with self.metrics_collector_context():
            self.start_metrics()
            rec = run_query(config)

        record_result(rec, rec['result_filename'])

        if rec['command_timed_out']:
            raise TimeoutException()
        if rec['error']:
            raise Exception('Query failed')

class SqlBatchTest(SqlTest):
    def __init__(self, test_config, default_configs=_default_configs):
        super(SqlBatchTest, self).__init__(test_config, default_configs=default_configs)

    def run_test(self):
        config = self.test_config

        config['root_test_uuid'] = config['test_uuid']
        child_messages = {}

        # Create random query list for each stream
        config['query_filenames'] = sorted(glob_file_list(config['query_filespec']))
        random.seed(config['random_seed'])
        stream_configs = []
        queries_per_stream = config.get('queries_per_stream',0)
        for stream_id in range(0, config.get('stream_count',1)):
            stream_config = config.copy()
            stream_config['stream_id'] = stream_id
            if config['random_seed'] != 0:
                random.shuffle(stream_config['query_filenames'])
            if queries_per_stream > 0:
                stream_config['query_filenames'] = stream_config['query_filenames'][0:queries_per_stream]
            logging.info('Queries for stream %d: %s' % (stream_config['stream_id'], ' '.join(stream_config['query_filenames'])))
            stream_configs.append(stream_config)

        self.hadoop_authenticate()
        self.configure_environment()

        with self.metrics_collector_context():
            self.start_metrics()
                
            error_count = 0
            success_count = 0
            t0 = datetime.datetime.utcnow()

            # Start stream processes
            active_streams = {}
            queue = multiprocessing.Queue()
            for stream_config in stream_configs:
                stream_config = stream_config.copy()
                del stream_config['_status_node']   # We can't send this between processes.
                stream_id = stream_config['stream_id']
                process = multiprocessing.Process(target=run_query_stream, args=(queue, stream_config))
                process.start()
                active_streams[stream_id] = {'process': process, 'stream_config': stream_config}
                
            # Monitor stream processes
            while len(active_streams.keys()) > 0:
                # Update status
                status_text = 'successful queries=%d, errors=%d' % (success_count, error_count)
                status_node = config['_status_node']
                status_node.set_status(status_text, destroy_children=False)

                # Handle any completed stream processes
                for stream_id in active_streams.keys():
                    process = active_streams[stream_id]['process']
                    if not process.is_alive():
                        logging.info('Stream %d is done' % stream_id)
                        process.join()
                        return_code = process.exitcode
                        if return_code != 0:
                            # An uncaught exception has occured. Normal query failures are not handled here.
                            logging.error('Stream %d returned error %d' % (stream_id, return_code))
                            error_count += 1
                        del active_streams[stream_id]

                # Process messages (individual query results, stream results) from stream processes
                try:
                    while True:
                        # Wait up to 1 second for next message in queue.
                        message = queue.get(True, 1)
                        # Create a new test_uuid for this child record.
                        # The query batch test_uuid is in root_test_uuid.
                        message['record_uuid'] = str(uuid.uuid4())
                        message['test_uuid'] = message['record_uuid']
                        # Record individual message to a file for immediate visibility.
                        record_result(message, message['result_filename'])
                        # Also add to child_messages key of the query batch record.
                        record_type = message['record_type']
                        if record_type not in child_messages:
                            child_messages[record_type] = []
                        child_messages[record_type].append(message)
                        # Count successful and error queries.
                        if message['record_type'] == 'query_result':
                            if message['error']:
                                error_count += 1
                            else:
                                success_count += 1
                except Queue.Empty:
                    pass
                except KeyboardInterrupt:
                    raise
                except:
                    logging.error('Unexpected error: %s' % sys.exc_info()[0])

            t1 = datetime.datetime.utcnow()
            td = t1 - t0
            logging.info('All streams are done')

        rec = config.copy()
        rec['record_uuid'] = rec['test_uuid']
        rec['record_type'] = 'query_batch_summary'
        rec['utc_begin'] = t0.isoformat()
        rec['utc_end'] = t1.isoformat()
        rec['elapsed_sec'] = time_duration_to_seconds(td)
        rec['error'] = (error_count > 0)
        rec['child_messages'] = child_messages
        record_result(rec, rec['result_filename'])

        logging.info('successful queries=%d, errors=%d' % (success_count, error_count))

        if rec['error']:
            raise Exception('Query batch failed')

def run_query_stream(queue, stream_config):
    stream_id = stream_config['stream_id']
    logging.info('%d: Stream begin' % stream_id)
    t0 = datetime.datetime.utcnow()    
    stream_error = False

    for query_index, query_filename in enumerate(stream_config['query_filenames']):
        logging.info('%d: query_index=%d, query_filename=%s' % (stream_id, query_index, query_filename))
        query_config = stream_config.copy()
        del query_config['query_filenames']
        query_config['query_index'] = query_index
        query_config['query_filename'] = query_filename
        run_query(query_config)
        if query_config['error']: stream_error = True
        # Place query_result record in queue. These will be collected and recorded by SqlBatchTest.run_test().
        queue.put(query_config)
        
    t1 = datetime.datetime.utcnow()
    td = t1 - t0
    rec = stream_config.copy()
    rec['record_type'] = 'query_stream_summary'
    rec['utc_begin'] = t0.isoformat()
    rec['utc_end'] = t1.isoformat()
    rec['elapsed_sec'] = time_duration_to_seconds(td)
    rec['error'] = stream_error
    # Place query_stream_summary record in queue. These will be collected and recorded by SqlBatchTest.run_test().
    queue.put(rec)
    
    logging.info('%d: Stream end' % stream_id)

def run_query(query_config):
    rec = query_config
    print_output = rec.get('print_output',True)
    stream_id = rec.get('stream_id', 0)

    rec['db_name'] = rec['db_name'] % rec

    if rec.get('kill_all_yarn_jobs_before_each_query',False):
        kill_all_yarn_jobs()

    rec['query_filename_contents'] = read_file_to_string(rec['query_filename'])
    
    shell = False
    db_type = rec['db_type']

    # Build query command.

    if db_type == 'hawq':
        cmd = []
        cmd.extend(['psql'])
        cmd.extend(['-v', 'ON_ERROR_STOP=1'])
        cmd.extend(['-d', rec['db_name']])
        cmd.extend(['-tAf', rec['query_filename']])

    elif db_type == 'hive':
        if not 'hiveconf:hive.tez.java.opts' in rec and 'java_opts_xmx_ratio' in rec and 'hiveconf:hive.tez.container.size' in rec:
            rec['hiveconf:hive.tez.java.opts'] = '-Xmx%dm' % (rec['hiveconf:hive.tez.container.size'] * rec['java_opts_xmx_ratio'])
        hiveconf = []
        for k,v in rec.items():
            prop = regex_first_group('^hiveconf:(.*)', k)
            if prop:
                hiveconf.extend(['--hiveconf','"%s=%s"' % (prop, v)])
        cmd = []
        cmd.extend(['hive'])
        cmd.extend(['--database', rec['db_name']])
        cmd.extend(['-f', rec['query_filename']])
        if 'hive_init_file' in rec:
            cmd.extend(['-i', rec['hive_init_file']])
            # Record contents of file in result.
            rec['hive_init_file_contents'] = read_file_to_string(rec['hive_init_file'])
        cmd.extend(hiveconf)

    elif db_type == 'impala':
        cmd = []
        cmd.extend(['impala-shell'])
        cmd.extend(['--impalad', '%s:%d' % (rec.get('impalad_host','localhost'), rec.get('impalad_port',21000))])
        cmd.extend(['--database', rec['db_name']])
        cmd.extend(['-f', rec['query_filename']])
        cmd.extend(['-B'])  # turn off pretty printing
        cmd.extend(['-o', '/dev/null'])
        if rec.get('profile_query'):
            cmd.extend(['--show_profiles'])

    else:
        raise('Unknown db_type')
    
    logging.info('%d: # %s' % (stream_id, ' '.join(cmd)))
    rec['query_command'] = cmd

    t0 = datetime.datetime.utcnow()
    
    # Run query.

    return_code, output, errors = system_command(cmd, 
        print_command=False, 
        print_output=print_output, 
        timeout=rec.get('command_timeout_sec',None), 
        raise_on_error=False,
        shell=shell)

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
    rec['record_type'] = 'query_result'

    # Parse query output to determine elapsed time and rows returned.

    if db_type == 'hive':
        rec['application_id'] = regex_first_group('\\(Executing on YARN cluster with App id (application_.*)\\)$', 
            errors, return_on_no_match=None, search=True, flags=re.MULTILINE)

        # Extract actual query duration from stderr text. Note that we must find the last occurance of 'Time taken'.
        query_elapsed_sec = regex_first_group(
            'Time taken: ([0-9.]+) seconds',
            errors, return_on_no_match='nan', search=True, flags=re.MULTILINE, match_last=True)
        if query_elapsed_sec == 'nan':
            logging.warn('Time taken not returned by command.')
            rec['error'] = True
        rec['query_elapsed_sec'] = float(query_elapsed_sec)
        rec['non_query_elapsed_sec'] = rec['elapsed_sec'] - rec['query_elapsed_sec']

        # Extract row count from stderr text. Note that some queries will not report fetched rows.
        query_rows_returned = regex_first_group(
            'Fetched: ([0-9]+) row',
            errors, return_on_no_match='0', search=True, flags=re.MULTILINE)
        rec['query_rows_returned'] = int(query_rows_returned)

        logging.info('error=%d, query_elapsed_sec=%f, non_query_elapsed_sec=%f, query_rows_returned=%d' % 
            (rec['error'], rec['query_elapsed_sec'], rec['non_query_elapsed_sec'], rec['query_rows_returned']))

    elif db_type == 'impala':
        # Extract actual query duration from stderr text.
        # Fetched 100 row(s) in 0.98s
        query_elapsed_sec = regex_first_group(
            'Fetched [0-9]+ row\\(s\\) in ([0-9.]+)s',
            errors, return_on_no_match='nan', search=True, flags=re.MULTILINE, match_last=True)
        if query_elapsed_sec == 'nan':
            logging.warn('Time taken not returned by command.')
            rec['error'] = True
        rec['query_elapsed_sec'] = float(query_elapsed_sec)
        rec['non_query_elapsed_sec'] = rec['elapsed_sec'] - rec['query_elapsed_sec']

        # Extract row count from stderr text. Note that some queries will not report fetched rows.
        query_rows_returned = regex_first_group(
            'Fetched ([0-9]+) row\\(s\\)',
            errors, return_on_no_match='0', search=True, flags=re.MULTILINE)
        rec['query_rows_returned'] = int(query_rows_returned)

        logging.info('error=%d, query_elapsed_sec=%f, non_query_elapsed_sec=%f, query_rows_returned=%d' % 
            (rec['error'], rec['query_elapsed_sec'], rec['non_query_elapsed_sec'], rec['query_rows_returned']))

    else:
        rec['query_elapsed_sec'] = rec['elapsed_sec']
        rec['non_query_elapsed_sec'] = 0.0
        rec['query_rows_returned'] = np.nan

    # Handle errors.

    if rec['error']:
        logging.info('%d: return_code=%d' % (stream_id, return_code))
        if not print_output:
            logging.info('%d: %s' % (stream_id, output))
            
        if db_type == 'hive':
            # Kill YARN application
            if rec['application_id']:
                kill_yarn_job(rec['application_id'])
                
    if errors != '':
        if not print_output:
            logging.info('%d: %s' % (stream_id, errors))

    if not rec['error']:
        logging.info('%d: %s: %0.3f seconds' % (stream_id, rec['query_filename'], rec['elapsed_sec']))
        
    return rec
