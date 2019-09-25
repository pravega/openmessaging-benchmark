# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import logging

class StorageBase:
    def __init__(self, config):
        self.config = config

    def configure(self):
        pass

    def get_info(self):
        pass

    def flush_cache(self):
        pass
