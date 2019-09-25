#!/usr/bin/env python
# Written by Claudio Fahey (claudio.fahey@emc.com)
# Run under Python 2.6.6 or higher 2.x.

from __future__ import division
import subprocess
import sys
import os
import shutil
import json
import uuid
import datetime
import time
import logging
import optparse
import traceback

# P3 Libraries
from p3_util import record_result, regex_first_group, public_dict, mkdir_for_file
from json_util import load_json_from_file
import p3_plugin_manager
from application_status_tree import StatusTreeServer

app_ver = '2.0'
global_error_count = 0;
global_warning_count = 0

default_config = {
    "max_test_attempts": 1,
    "noop": False
}

def run_test(config):
    global global_error_count
    global global_warning_count
    test = config['test']
    logging.info('Running test: %s' % test)
    config['test_uuid'] = str(uuid.uuid4())
    config['utc_begin'] = datetime.datetime.utcnow().isoformat()    # May be overwritten by actual test.

    pct_complete = 100.0 * (config['sequence_in_test_batch'] - 1) / config['size_of_test_batch']
    config['test_desc'] = 'Running test \'%s\': %d of %d (%.0f %%)' % (
        test, config['sequence_in_test_batch'], config['size_of_test_batch'], pct_complete)
    if global_error_count > 0 or global_warning_count > 0:
        config['test_desc'] += ' (%d errors, %d warnings)' % (global_error_count, global_warning_count)

    logging.info('*****************************************************************');
    logging.info(config['test_desc'])

    status_node = config['_status_node']
    status_node.set_status(config['test_desc'], destroy_children=True)
    child_status_node = config['_status_node'].create_child()
    config['_status_node'] = child_status_node

    # Create test instance now so that the final test configuration is built with defaults, etc.
    test_class = p3_plugin_manager.get_class('test', test)
    test_class_instance = test_class(config)

    logging.info('Running test \'%s\' with the following configuration:' % test)
    logging.info(json.dumps(public_dict(config), sort_keys=True, indent=4, ensure_ascii=False))

    test_class_instance.run_test()

    logging.info('Completed test: %s' % test)

def run_all_tests(configs, skip=0, num_tests=None, status_tree=None):
    global global_error_count
    global global_warning_count
    if num_tests:
        last = skip + num_tests
        if last < len(configs):
            configs = configs[:last]
    num_tests = len(configs)
    logging.info('Number of tests: %d' % num_tests)
    status_tree.set_status('Running %d tests' % num_tests, destroy_children=True)
    status_node = status_tree.create_child()

    test_batch_uuid = str(uuid.uuid4())

    try:
        for i, config in enumerate(configs):
            if i >= skip:
                config['sequence_in_test_batch'] = i + 1  # first test will be 1
                config['size_of_test_batch'] = num_tests
                config['test_attempt'] = 0
                config['test_batch_uuid'] = test_batch_uuid
                                
                stop = False
                while not stop:
                    try:
                        config['test_attempt'] += 1
                        # Must create copy so that any additional attempts have a virgin config.
                        config_copy = config.copy()
                        config_copy['_status_node'] = status_node
                        run_test(config_copy)
                        stop = True;
                    except Exception as e:
                        global_warning_count += 1
                        logging.info('EXCEPTION: %s' % traceback.format_exc())
                        logging.info('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                        if config['test_attempt'] >= config['max_test_attempts']:
                            logging.info('All attempts failed!')
                            global_error_count += 1
                            stop = True

        logging.info('*****************************************************************')
        logging.info('All %d tests completed. (%d errors, %d warnings)' % (num_tests, global_error_count, global_warning_count))
        logging.info('Test Batch UUID: %s' % test_batch_uuid)
    except KeyboardInterrupt:
        logging.info('*****************************************************************')
        logging.warning('ABORTED BY KEYBOARD INTERRUPT')
        logging.info('%d of %d tests completed. (%d errors, %d warnings)' % (i, num_tests, global_error_count, global_warning_count))
        logging.info('Test Batch UUID: %s' % test_batch_uuid)

def main():
    print('%s version %s' % (os.path.basename(__file__), app_ver))

    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', action='append', dest='config', help='file containing common configuration for all tests')
    parser.add_option('-t', '--tests', action='append', dest='tests', help='file containing tests to run; use "-" for stdin')
    parser.add_option('-s', '--skip', type='int', action='store', dest='skip', default=0, help='skip this number of tests')
    parser.add_option('-n', '--num-tests', type='int', action='store', dest='num_tests', help='run only this number of tests')
    parser.add_option('-d', '--dump-test-configs', action='store_true', dest='dump_test_configs', help='show test details and exit (don\'t run)')
    options, args = parser.parse_args()

    if not options.tests:
        parser.error('required parameters not specified')

    if not options.config:
        options.config = {}
    common_config = {}
    for config_filename in options.config:
        try:
            common_config.update(load_json_from_file(config_filename))
        except Exception as e:
            logging.error('Unable to parse file %s' % config_filename)
            raise

    # Initialize logging
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.DEBUG)
    consoleHandler = logging.StreamHandler(sys.stdout)
    logging.Formatter.converter = time.gmtime
    consoleHandler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    rootLogger.addHandler(consoleHandler)
    if 'test_driver_log_filename' in common_config:
        filename = common_config['test_driver_log_filename']
        print('Logging to file %s' % filename)
        mkdir_for_file(filename)
        fileHandler = logging.FileHandler(filename)
        fileHandler.setFormatter(logging.Formatter('%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s] %(message)s'))
        rootLogger.addHandler(fileHandler)
        logging.info('BEGIN')
        logging.info('%s version %s' % (os.path.basename(__file__), app_ver))

    # Load plugins
    p3_plugin_manager.scan_plugins()

    # Build test configs
    configs = []
    for tests_filename in options.tests:
        if tests_filename == '-':
            try:
                file_test_configs = json.load(sys.stdin)
            except Exception as e:
                logging.error('Unable to parse stdin')
                raise
        else:
            try:                
                file_test_configs = load_json_from_file(tests_filename)
            except Exception as e:
                logging.error('Unable to parse file %s' % tests_filename)
                raise
        file_common_config = {}
        for test_config in file_test_configs:
            if '_COMMON_FILE_CONFIG' in test_config and test_config['_COMMON_FILE_CONFIG']:
                del test_config['_COMMON_FILE_CONFIG']
                file_common_config.update(test_config)
            elif 'test' in test_config:
                test = test_config['test']
                c = {}
                c.update(default_config)                       # Apply default config
                c.update(common_config)                        # Apply common config from -c files
                c.update(file_common_config)                   # Apply config from this test file where _COMMON_FILE_CONFIG=true
                c.update(test_config)                          # Apply config from this test
                c['test_driver_version'] = app_ver
                configs.append(c)

    if options.dump_test_configs:
        full_configs = []
        for test_config in configs:
            test_class = p3_plugin_manager.get_class('test', test_config['test'])
            test_class_instance = test_class(test_config)
            full_configs.append(test_config)
        logging.info('There are %d tests:' % len(configs))
        logging.info('\n%s' % json.dumps(full_configs, sort_keys=True, indent=4, ensure_ascii=False))
    else:
        status_tree = StatusTreeServer(status_file=common_config.get('status_html',None))
        with status_tree.context():
            run_all_tests(configs, skip=options.skip, num_tests=options.num_tests, status_tree=status_tree)

    logging.info('END')

if __name__ == '__main__':
    main()
