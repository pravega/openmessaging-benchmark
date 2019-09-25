#!/usr/bin/env python

from __future__ import print_function
import json
import sys
import glob

test_list = []

data_size_MB = 1000*1000

config = {
    "base_directory": "%(storage_hadoop_uri)s/benchmarks/terasort-%(data_size_MB)0.0f",
    "block_size_MiB": 512,
    "command_timeout_sec": 1200,
    "data_size_MB": data_size_MB,
    "map_cores": 1,
    "map_max_attempts": 1,
    "map_output_compress_codec": "org.apache.hadoop.io.compress.Lz4Codec",
    "max_test_attempts": 1,
    "optimizer_extra_points_file": "../data/optimizer/%(optimizer_uuid)s-extra.json",
    "optimizer_grid_search": False,
    "optimizer_hyperparameter_alpha_domain": [0.09, 0.11],
    "optimizer_max_moe_iterations": 10000,
    "optimizer_max_num_points_since_hyper_opt": 0,
    "optimizer_max_test_attempts": 2,
    "optimizer_min_sample_size": 2,
    "optimizer_minimize_col_stddev_domain": [10.0, None],
    "optimizer_moe_rest_host": "hop-claudio-ubuntu-desktop.solarch.lab.emc.com",
    "optimizer_observations_file": "../data/optimizer/%(optimizer_uuid)s-obs.json",
    "optimizer_transform_col_rules":
        [
            {   "name": "flush_compute", "type": "boolean", "grid_search": [False], "fixed_value": False},
            {   "name": "io_file_buffer_size",
                    "type": "log", "data_type": "int64", "min": 4096, "max": 16*1024*1024, 
                    "base": 2, "exponent_multiple_of": 1, "multiple_of": 4096, 
                    "grid_search": [128*1024], "fixed_value": 128*1024},
            {   "name": "map_memory_MB",
                    "type": "linear", "data_type": "int64", "min": 1024, "max": 32*1024, "multiple_of": 1024, 
                    "grid_search": [2*1024], "fixed_value": 2*1024},
            {   "name": "map_tasks", 
                    "type": "linear", "data_type": "int64", "min": 12, "max": 256, 
                    "hyperparameter_min": 9.0, "hyperparameter_max": 14.0,
                    "grid_search": [12,64,128,256]},
            {   "name": "transparent_hugepage_enabled", "type": "boolean", "grid_search": [False], "fixed_value": False},
        ],
    "optimizer_to_minimize": "elapsed_sec",
    "optimizer_test": "teragen",
    "optimizer_uuid": "549adb81-f249-41a7-af93-4e1987570654",   # Generate with: python -c 'import uuid; print(uuid.uuid4())'
    "reduce_max_attempts": 1,
    "result_filename": "../data/mapred_results/json/%(cluster_name)s_%(storage_cluster_name)s_%(test)s_%(timestamp)s_%(test_uuid)s.json.bz2",
    "test": "optimizer",
    }

test_list.append(config)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
