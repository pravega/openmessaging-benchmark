#!/usr/bin/env python

from __future__ import print_function
import json
import sys
import itertools

sys.path.append(".")
from p3_util import grid_to_config_list

test_list = []

for common in grid_to_config_list([
        {"isilon_num_nodes": [3]},
        {None: range(0,3)},
        {"data_size_MB": [1000*1000]},
        {"num_compute_nodes": [None]},
        {"block_size_MiB": [512]},
        {"flush_compute": [True]},
        {"io_file_buffer_size": [128*1024]},
        {"isilon_data_access_pattern": ['streaming']},
        {"isilon_flush": [True]},
        {"isilon_hdfs_server_threads": [80]},   # X410 auto = 80
        {"isilon_protection_level": ['2d_1n']},
        {"isilon_smart_cache_enabled": [True]},
        {"map_cores": [1]},                     # To oversubscribe CPUs, set yarn.nodemanager.resource.cpu-vcores to higher than physical count
        {"map_max_attempts": [1]},
        {"map_memory_MB": [128]},
        {"map_output_compress_codec": ["org.apache.hadoop.io.compress.Lz4Codec"]},
        {"map_tasks_per_storage_node": [512,256,128,96,32,8,16,24,40,48,56,60]},     # 32,8,16,24,40,48,56,60
        {"max_test_attempts": [1]},
        {"reduce_max_attempts": [1]},
        {"result_filename": ["../data/mapred_results/json/%(cluster_name)s_%(storage_cluster_name)s_%(test)s_%(timestamp)s_%(test_uuid)s.json.bz2"]},
        {"transparent_hugepage_enabled": [False]},
        ]):
    common['map_tasks'] = common['map_tasks_per_storage_node'] * common['isilon_num_nodes']
    common.update({
        "base_directory": '/benchmarks/%s-%s%s/dfsio-%d-%d' % (
            common['isilon_data_access_pattern'], common['isilon_protection_level'], '' if common['isilon_smart_cache_enabled'] else '-0',
            common['map_tasks'], common['data_size_MB']),
        })

    # Write
    for config in grid_to_config_list([
            {"repeat": range(0,1)},
            {"test": ["write"]},
            ]):
        t = common.copy()
        t.update(config)
        test_list.append(t)

    # Read
    for config in grid_to_config_list([
            {"repeat": range(0,2)},
            {"test": ["read"]},
            ]):
        t = common.copy()
        t.update(config)
        test_list.append(t)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
