# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division

import json
import logging
import requests

# P3 Libraries
import p3_plugin_manager
from p3_storage import StorageBase
from system_command import ssh
from p3_util import unflatten_dict_keys, regex_first_group

class PluginInfo(p3_plugin_manager.IP3Plugin):
    def get_plugin_info(self):
        return [
            {'class_type': 'storage', 'class_name': 'isilon', 'class': IsilonStorage},
            ]

class IsilonStorage(StorageBase):
    def configure(self):
        config = self.config

        self.get_isilon_version()

        # TODO: Need to set HDFS block size for specific access zone.
        block_size_MiB = config.get('block_size_MiB', None)
        if block_size_MiB:
            self.run_isilon_command('isi hdfs settings modify --default-block-size %dMB' % block_size_MiB)

        # Note that --server-threads is not supported on OneFS 8.0.
        isilon_hdfs_server_threads = config.get('isilon_hdfs_server_threads', None)
        if isilon_hdfs_server_threads:
            self.run_isilon_command('isi hdfs settings modify --server-threads %d' % isilon_hdfs_server_threads)

        # Note that --server-log-level is not supported on OneFS 8.0.
        isilon_hdfs_log_level = config.get('isilon_hdfs_log_level', None)
        if isilon_hdfs_log_level:
            self.run_isilon_command('isi hdfs settings modify --server-log-level %s' % isilon_hdfs_log_level)

        sysctl_settings = unflatten_dict_keys(config, 'isilon_sysctl_(.*)')
        if sysctl_settings:
            args = ['isi_sysctl_cluster %s="%s"' % (k,v) for k,v in sysctl_settings.iteritems()]
            cmd = ' && '.join(args)
            self.run_isilon_command(cmd)

        # TODO: Need to bring in code for resizing cluster from hadoop-test-driver.pl

        requested_isilon_num_nodes = config.get('isilon_num_nodes')
        if requested_isilon_num_nodes:
            self.get_isilon_node_info()
            actual_isilon_num_nodes = self.get_num_isilon_nodes(config['isilon_node_info'])
            logging.info('IsilonStorage.configure: actual_isilon_num_nodes=%d' % actual_isilon_num_nodes)
            if actual_isilon_num_nodes != requested_isilon_num_nodes:
                raise Exception('Actual isilon_num_nodes (%d) does not match requested isilon_num_nodes (%d)' % (actual_isilon_num_nodes, requested_isilon_num_nodes))

        self.get_info()
        
        self.flush_cache()

    def get_info(self):
        config = self.config
        self.get_isilon_node_info()
        self.get_isilon_access_zone_info()
        self.get_isilon_network_interface_info()
        config['isilon_num_nodes'] = self.get_num_isilon_nodes(config['isilon_node_info'])
        config['storage_num_nodes'] = config['isilon_num_nodes']
        if self.have_minimum_isilon_version((8,0)):
            config['isilon_onefs_patches'] = json.loads(self.run_isilon_command('isi upgrade patches list --format json')[1])
        else:
            config['isilon_onefs_pkg_info'] = self.run_isilon_command('isi pkg info')[1]
        config['isilon_hdfs_racks'] = json.loads(self.run_isilon_command('isi hdfs racks list --format json')[1])
        config['isilon_status'] = self.run_isilon_command('isi status')[1]
        config['isilon_job_status'] = self.run_isilon_command('isi job status')[1]

    def flush_cache(self):
        config = self.config
        if not config['noop'] and config['isilon_flush']:
            self.run_isilon_command('isi_for_array isi_flush')

    def run_isilon_command(self, cmd, *args, **kwargs):
        config = self.config
        assert 'isilon_user' in config
        assert 'isilon_host' in config
        if config['noop']:
            logging.info('# ssh ' % cmd)
            return 0, ''
        else:
            return ssh(config['isilon_user'], config['isilon_host'], cmd, stderr_to_stdout=False, *args, **kwargs)

    def get_isilon_node_info(self, force=False):
        config = self.config
        if force or 'isilon_node_info' not in config:
            base_isilon_url = 'https://%s:8080' % config['isilon_host']
            url = '%s/platform/1/storagepool/nodepools/%s' % (base_isilon_url, config['isilon_node_pool_name'])
            info = requests.get(url, auth=(config['isilon_user'], config['_isilon_password']), verify=False).json()
            config['isilon_node_info'] = info
        return config['isilon_node_info']

    def get_isilon_access_zone_info(self):
        config = self.config
        base_isilon_url = 'https://%s:8080' % config['isilon_host']
        url = '%s/platform/1/zones' % base_isilon_url
        info = requests.get(url, auth=(config['isilon_user'], config['_isilon_password']), verify=False).json()
    #    Print(json.dumps(info, sort_keys=True, indent=4, ensure_ascii=False))
        config['isilon_access_zone_info'] = info
        return info

    def get_isilon_network_interface_info(self):
        config = self.config
        if self.have_minimum_isilon_version((8,0)):
            cmd = 'isi network interfaces list --verbose --show-inactive'
        else:
            cmd = 'isi networks list interfaces --verbose --wide --show-inactive'
        exit_code, output = ssh(config['isilon_user'], config['isilon_host'], cmd, print_output=False)
        if exit_code != 0:
            raise Exception('Unable to get Isilon network interface info.')
        config['isilon_network_interface_info'] = output
        return output

    def get_isilon_node_ids(self, isilon_node_info):
        return sorted(isilon_node_info['nodepools'][0]['lnns'])

    def get_num_isilon_nodes(self, isilon_node_info):
        count = len(self.get_isilon_node_ids(isilon_node_info))
        logging.info('get_num_isilon_nodes: count=%d' % count)
        return count

    def get_isilon_version(self):
        config = self.config
        if not 'isilon_onefs_version' in config:
            config['isilon_onefs_version'] = self.run_isilon_command('isi version')[1].replace('\n','')
            version_tuple = self.get_isilon_version_tuple(config['isilon_onefs_version'])
            logging.debug('Isilon version tuple=%s' % str(version_tuple))
        return config['isilon_onefs_version']

    def get_isilon_version_tuple(self, isi_version_output):
        def try_int(x):
            try:
                return int(x)
            except:
                return x            
        s = regex_first_group('.*Isilon OneFS v(.*?) ', isi_version_output)
        return tuple(try_int(d) for d in s.split('.'))

    def have_minimum_isilon_version(self, version_tuple):
        return self.get_isilon_version_tuple(self.get_isilon_version()) >= version_tuple

