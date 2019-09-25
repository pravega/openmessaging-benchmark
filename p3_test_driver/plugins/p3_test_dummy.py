# Bare-bones test for P3 Test Driver.

import time

import p3_plugin_manager
from p3_test import BaseTest

class PluginInfo(p3_plugin_manager.IP3Plugin):
    def get_plugin_info(self):
        return [
            {
            'class_type': 'test', 
            'class_name': 'dummytest', 
            'class': DummyTest,
            },
            ]

class DummyTest(BaseTest):
    def __init__(self, test_config):
        default_configs = {
            'all': {'a': 'from all', 'b': 'from all', 'd': 'from all'},
            'dummytest': {'a': 'from dummytest', 'c': 'from dummytest'},
            }
        super(DummyTest, self).__init__(test_config, default_configs)

    def run_test(self):
        config = self.test_config
        print('DummyTest.run_test')
        config['_status_node'].set_status('this is DummyTest.run_test')
        time.sleep(config.get('sleep_sec',5))
