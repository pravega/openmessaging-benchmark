
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
import math
import json
import traceback
import pandas as pd
import numpy as np
import random
from contextlib import contextmanager

# P3 Libraries
import p3_plugin_manager
from p3_util import regex_first_group, regex_groups, record_result, updated_dict, read_file_to_string, public_dict
from system_command import popen_to_queue, system_command, ssh, time_duration_to_seconds
from p3_test import BaseTest, TimeoutException
from optimizer import Optimizer, slim_results_dict

_default_configs = {
    'optimizer': {
        'optimizer_grid_search': True,
        'optimizer_hyperparameter_alpha_domain': [0.02, 4.0],
        'optimizer_max_get_point_attempts': 30,
        'optimizer_max_moe_iterations': 1000,
        'optimizer_max_num_points_since_hyper_opt': 5,
        'optimizer_max_test_attempts': 1,
        'optimizer_min_sample_size': 3,
        'optimizer_minimize_col_stddev_domain': [None, None],
        'optimizer_moe_rest_host': '127.0.0.1',
        },    
    }

class PluginInfo(p3_plugin_manager.IP3Plugin):
    def get_plugin_info(self):
        return [
            {
            'class_type': 'test', 
            'class_name': 'optimizer',
            'class': OptimizerTest,
            },
            {
            'class_type': 'test', 
            'class_name': 'optimizer_unit_test',
            'class': OptimizerUnitTestTest,
            },
        ]

# Set global option to disable line wrap in Pandas
pd.set_option('display.width', None)

class OptimizerTest(BaseTest):
    def __init__(self, test_config, default_configs=_default_configs):
        new_default_configs = default_configs.copy()
        new_default_configs['optimizer']['optimizer_max_test_attempts'] = test_config['max_test_attempts']
        super(OptimizerTest, self).__init__(test_config, default_configs=new_default_configs)
        self.exception_count = 0
        self.timeout_count = 0

    def run_test(self):
        config = self.test_config

        observations_file = config.get('optimizer_observations_file','') % config
        extra_points_file = config.get('optimizer_extra_points_file','') % config

        self.optimizer = Optimizer(
            transform_col_rules=config['optimizer_transform_col_rules'], 
            hyperparameter_alpha_domain=config['optimizer_hyperparameter_alpha_domain'],
            to_minimize=config['optimizer_to_minimize'],
            observations_file=observations_file,
            extra_points_file=extra_points_file,
            grid_search=config['optimizer_grid_search'],
            max_get_point_attempts=config['optimizer_max_get_point_attempts'],
            max_num_points_since_hyper_opt=config['optimizer_max_num_points_since_hyper_opt'],
            min_sample_size=config['optimizer_min_sample_size'],
            max_moe_iterations=config['optimizer_max_moe_iterations'],
            moe_rest_host=config['optimizer_moe_rest_host'],
            minimize_col_stddev_domain=config['optimizer_minimize_col_stddev_domain']
            )

        self.optimizer.load()

        while True:
            # Ask optimizer for the point to sample next
            logging.info('OptimizerTest.run_test: Determining next point to sample')
            next_point_to_sample, needed_new_observations, optimization_info = self.optimizer.next_point_to_sample()
            if next_point_to_sample is None:
                break

            # Build sample config and run the internal test multiple times to build a sample of the specified sample size.
            sample_config = config.copy()
            sample_config.update(optimization_info)
            sample_config.update(next_point_to_sample)
            sample_config['_optimizer_point_to_sample'] = next_point_to_sample
            sample_config['max_test_attempts'] = config['optimizer_max_test_attempts']
            sample_config['optimizer_avoid_point'] = False
            sample_config['optimizer_needed_new_observations'] = needed_new_observations

            best_sample = self.optimizer.best_sample
            if not best_sample is None:
                sample_config['optimizer_best_%s_mean' % self.optimizer.minimize_col] = best_sample['mean']
                sample_config['optimizer_best_%s_std' % self.optimizer.minimize_col] = best_sample['std']

            sample_config['test'] = config['optimizer_test']
            self.run_sample(sample_config)

    def run_sample(self, sample_config):
        """Run multiple observations to obtain a sample of specified sample size."""
        # Build test config with this point
        observation_config = sample_config.copy()
        observation_config['optimizer_sequence_in_sample'] = 0      # first observation will be 1
        num_new_observations = 0
        stop = False
        while not stop:
            observation_config['optimizer_sequence_in_sample'] += 1            
            observation_record = self.run_observation_with_retry(observation_config)
            if observation_record['optimizer_avoid_point']:
                stop = True
            else:
                num_new_observations += 1
                if num_new_observations >= observation_record['optimizer_needed_new_observations']:
                    stop = True

    def run_observation_with_retry(self, config):
        """Run 1 or more test attempts to get an observation. If no attempts succeed, then the point will be marked to avoid."""
        config['optimizer_test_attempt'] = 0   # First attempt will be 1.
                        
        stop = False
        success = False

        while not stop:
            config['optimizer_test_attempt'] += 1
            # Must create copy so that all observations/attempts have a virgin config.
            observation_record = config.copy()

            try:
                self.run_observation(observation_record)
            except TimeoutException as e:
                logging.error('!' * 100)
                logging.error('EXCEPTION: Timed out: %s' % traceback.format_exc())
                self.timeout_count += 1
                # Don't retry on timeout errors.
                stop = True
            except Exception as e:
                # TODO: Distinguish between exceptions due to parameters or not due to parameters
                # For now, avoid any points that produce an exception.
                logging.error('!' * 100)
                logging.error('EXCEPTION: %s' % traceback.format_exc())
                self.exception_count += 1
            else:
                # Test completed without exception. Check error flag.
                if observation_record.get('error'):
                    logging.error('!' * 100)
                    logging.error('ERROR: Test returned an error')
                    self.exception_count += 1
                else:
                    stop = True
                    success = True

            if not success:
                if stop or config['optimizer_test_attempt'] >= config['optimizer_max_test_attempts']:
                    logging.info('All attempts failed!')
                    observation_record['error'] = True
                    observation_record['optimizer_avoid_point'] = True
                    stop = True

        assert observation_record['error'] == False or observation_record['optimizer_avoid_point'] == True

        slim_record = slim_results_dict(public_dict(observation_record))
        #logging.debug('slim_record=%s' % json.dumps(slim_record, sort_keys=True, indent=4, ensure_ascii=False))

        self.optimizer.add_observations([slim_record])
        return observation_record

    def run_observation(self, observation_record):
        """Run a single test."""
        config = observation_record
        test = config['test']
        logging.info('Running test: %s' % test)
        config['test_uuid'] = str(uuid.uuid4())
        config['utc_begin'] = datetime.datetime.utcnow().isoformat()    # May be overwritten by actual test.

        # Update status.

        config['test_desc'] = 'Running test \'%s\': %s, observation %d of %d, attempt %d of %d' % (
            test, self.optimizer.get_status_str(), 
            config['optimizer_sequence_in_sample'], config['optimizer_needed_new_observations'],
            config['optimizer_test_attempt'], config['optimizer_max_test_attempts'])
        config['test_desc'] += ' [%d exceptions, %d timeouts, %d historical points to avoid]' % (
            self.exception_count, self.timeout_count, self.optimizer.get_avoid_point_count())

        logging.info('*' * 100);
        logging.info(config['test_desc'])

        observations_df = self.optimizer.get_observations_df()
        if observations_df.empty:
            describe_df = pd.DataFrame()
        else:
            describe_df = observations_df[[self.optimizer.minimize_col]].describe()
        table_classes = 'pure-table pure-table-striped'
        status_html = ("""
            <div>
                <div>Optimization Status</div>
                <div>%(status_df)s</div>
                <div>Samples (best 10)</div>
                <div>%(samples_df)s</div>
                <div>Observations (last 10)</div>
                <div>%(observations_df)s</div>
                <div>Observation Statistics</div>
                <div>%(obs_stats_df)s</div>
            </div>
            """ % {
            'status_df': self.optimizer.get_status_df().to_html(header=True, classes=table_classes),
            'samples_df': self.optimizer.get_samples_df().reset_index().head(10).T.to_html(header=False, classes=table_classes),
            'observations_df': observations_df.tail(10).sort(ascending=False).T.to_html(classes=table_classes),
            'obs_stats_df': describe_df.to_html(classes=table_classes),
            })

        status_node = config['_status_node']
        status_node.set_status(config['test_desc'], destroy_children=True, html=status_html)
        child_status_node = config['_status_node'].create_child()
        config['_status_node'] = child_status_node

        # Create test instance now so that the final test configuration is built with defaults, etc.
        test_class = p3_plugin_manager.get_class('test', test)
        test_class_instance = test_class(config)

        logging.info('Running test \'%s\' with the following configuration:' % test)
        logging.info(json.dumps(config['_optimizer_point_to_sample'], sort_keys=True, indent=4, ensure_ascii=False))
        #logging.info(public_dict(config))
        #logging.info(json.dumps(public_dict(config), sort_keys=True, indent=4, ensure_ascii=False))

        # Run test. Note that test may raise an exception or return with the error flag set.
        test_class_instance.run_test()

        logging.info('Completed test: %s' % test)
        
class OptimizerUnitTestTest(object):
    def __init__(self, test_config):
        self.test_config = test_config

    def run_test(self):
        # Ideal solution (?): 
        # DataFrame samples_df:
        # x0  x1  x2  x3  b0         mean           var  count
        # 1.0 2.6 0.3 0.7 True  -2.613330  1.620596e-06      5
        config = self.test_config
        config['utc_begin'] = datetime.datetime.utcnow().isoformat()
        config['utc_end'] = datetime.datetime.utcnow().isoformat()
        x = [config['x0'], config['x1'], config['x2'], config['x3']]
        b = [float(config['b0'])]
        if x[2] >  0.5 and random.random() < 0.5:
            config['y'] = 1
            raise Exception('bad parameter x[2]<0.4')
        else:
            y = -b[0] + math.sin(x[0]) * math.cos(x[1]) + math.cos(x[0] + x[1]) + (x[2]-0.27)**2 + (np.log10(x[3])-2.0)**2 + random.uniform(-0.002, 0.002)
        config['y'] = y
        logging.info('OptimizerUnitTestTest.run_test: x=%s, y=%f' % (x,y))
        config['error'] = False
