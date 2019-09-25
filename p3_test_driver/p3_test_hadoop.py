# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import logging

# P3 Libraries
import p3_plugin_manager
import p3_storage
from p3_metrics import MetricsCollector
from p3_test import StorageTest
from system_command import system_command
from hadoop_util import configure_compute

class HadoopTest(StorageTest):
    def __init__(self, test_config, default_configs={}):
        super(HadoopTest, self).__init__(test_config, default_configs=default_configs)

    def configure_environment(self):
        self.configure_storage()
        self.configure_compute()

    def configure_compute(self):
        config = self.test_config
        configure_compute(config)        

    def hadoop_authenticate(self):
        # TODO: Need to bring in code from hadoop-test-driver.pl
        pass

    def delete_hadoop_directory(self, directory):
        self.hadoop_authenticate()
        cmd = ['hadoop', 'fs', '-rm', '-r', '-f', '-skipTrash', directory]
        system_command(cmd, print_command=True, print_output=True, raise_on_error=True, shell=False)

    def hadoop_file_exists(self, filename):
        self.hadoop_authenticate()
        cmd = ['hadoop', 'fs', '-test', '-e', filename]
        returncode, output, errors = system_command(cmd, print_command=True, print_output=False, raise_on_error=False, shell=False)
        file_exists = (returncode == 0)
        logging.info('hadoop_file_exists(%s) = %d' % (filename, file_exists))
        return file_exists
