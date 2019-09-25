# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division

import json
import logging
import requests

# P3 Libraries
import p3_plugin_manager
from p3_storage import StorageBase
from system_command import ssh
import p3_util

class PluginInfo(p3_plugin_manager.IP3Plugin):
    def get_plugin_info(self):
        return [
            {'class_type': 'storage', 'class_name': 'ecs', 'class': ECSStorage},
            ]

class ECSStorage(StorageBase):
    def configure(self):
        config = self.config
        self.get_info()        
        self.flush_cache()

    def get_info(self):
        config = self.config
        self.get_storage_node_info()
        config['storage_version'] = self.run_storage_command('cat /etc/issue')[1]
        config['storage_docker_ps'] = self.run_storage_command('sudo docker ps')[1]

    def run_storage_command(self, cmd, *args, **kwargs):
        config = self.config
        assert 'storage_user' in config
        assert 'storage_host' in config
        if config['noop']:
            logging.info('# ssh ' % cmd)
            return 0, ''
        else:
            return ssh(config['storage_user'], config['storage_host'], cmd, *args, **kwargs)

    def get_storage_node_info(self, force=False):
        config = self.config
        return None

    def flush_cache(self):
        config = self.config
        if not config['noop'] and config['storage_flush']:
            raise Exception('flush_cache not supported')
