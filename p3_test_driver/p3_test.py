# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import logging
from contextlib import contextmanager

# P3 Libraries
import p3_plugin_manager
import p3_storage
from p3_metrics import MetricsCollector
from p3_util import record_result, regex_first_group

class TimeoutException(Exception):
    pass

class BaseTest(object):
    def __init__(self, test_config, default_configs={}):
        """Note that this class updates test_config in place with new parameters and results.
        default_configs is a dict from 'all' and/or the name of specific tests."""
        self.test_config = test_config
        # Update test_config with defaults
        test = test_config.get('test')
        default_config = {}
        default_config.update(default_configs.get('all',{}))
        default_config.update(default_configs.get(test,{}))
        default_config = dict([(k,v) for k,v in default_config.items() if k not in self.test_config])
        #logging.debug('BaseTest.__init__: default_config=%s' % default_config)
        self.test_config.update(default_config)

    def record_result(self):
        config = self.test_config
        if 'result_filename' in config:
            record_result(config, config['result_filename'])

class MetricsTest(BaseTest):
    def __init__(self, test_config, default_configs={}):
        super(MetricsTest, self).__init__(test_config, default_configs=default_configs)

    def _metrics_start(self):
        config = self.test_config
        config['metrics'] = {}
        self.metrics_collector = MetricsCollector(config['metrics'])
        self.metrics_collector.start()

    def _metrics_stop(self):
        self.metrics_collector.stop()

    def start_target_metrics_agents(self, agents_config, targets):
        for target in targets:
            for agent_id_template, agent_config in agents_config.items():
                agent_id = agent_id_template % target
                #logging.info('agent_id=%s, target=%s, agent_config=%s' % (agent_id, target, agent_config))
                cmd_dict = self.test_config.copy()
                cmd_dict.update(target)
                cmd = agent_config['start_cmd'] % cmd_dict
                stop_cmd = agent_config.get('stop_cmd')
                if stop_cmd:
                    stop_cmd = stop_cmd % cmd_dict
                logging.info('%s: start_cmd=%s' % (agent_id, cmd))
                logging.info('%s: stop_cmd=%s' % (agent_id, stop_cmd))
                results_format = agent_config.get('results_format', 'text')
                self.metrics_collector.add_agent(agent_id, cmd, stop_cmd, results_format)

    def start_metrics(self):
        config = self.test_config
        for group_key, group_info in config.items():
            group_name = regex_first_group('^metrics_group:(.*)', group_key)
            if group_name:
                logging.debug('Found metrics group key %s' % group_key)
                host_names = [None]
                host_names_key = group_info.get('host_names_key')
                if host_names_key:
                    host_names = config.get(host_names_key)
                    if not isinstance(host_names, list):
                        host_names = [host_names]
                logging.debug('host_names=%s' % host_names)
                self.start_target_metrics_agents(group_info['agents'], [{'hostname': x} for x in host_names])

    @contextmanager
    def metrics_collector_context(self):
        """Context manager to allow 'with' statement to start and stop agents and collector."""
        self._metrics_start()
        yield
        self._metrics_stop()

class StorageTest(MetricsTest):
    def __init__(self, test_config, default_configs={}):
        super(StorageTest, self).__init__(test_config, default_configs=default_configs)

    def storage(self):
        storage_type = self.get_storage_type()
        if storage_type:
            try:
                storage_class = p3_plugin_manager.get_class('storage', self.get_storage_type())
            except:
                logging.warning('No plugin for storage type %s.' % storage_type)
                storage_class = p3_storage.StorageBase
        else:
            storage_class = p3_storage.StorageBase
        return storage_class(self.test_config)

    def get_storage_type(self):
        config = self.test_config
        return config.get('storage_type', None)

    def configure_storage(self):
        self.storage().configure()
