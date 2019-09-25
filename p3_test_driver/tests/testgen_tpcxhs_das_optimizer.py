#!/usr/bin/env python

from __future__ import print_function
import json
import sys
import glob

test_list = []

config = {
    "app_master_memory_MB": 5*1024,
    "base_directory": "/benchmarks",
    "data_size_MB": 1000*1000,
    "jar": "../TPCx-HS_Kit_v1.3.0_external/TPCx-HS-Runtime-Suite/TPCx-HS-master.jar",
    "map_cores": 1,
    "map_max_attempts": 1,
    "map_output_compress_codec": "org.apache.hadoop.io.compress.Lz4Codec",
    "max_test_attempts": 2,
    "optimizer_extra_points_file": "../data/optimizer/%(optimizer_uuid)s-extra.json",
    "optimizer_hyperparameter_alpha_domain": [0.1, 0.2],
    "optimizer_max_moe_iterations": 10000,
    "optimizer_max_num_points_since_hyper_opt": 0,
    "optimizer_moe_rest_host": "hop-claudio-ubuntu-desktop",
    "optimizer_min_sample_size": 2,
    "optimizer_observations_file": "../data/optimizer/%(optimizer_uuid)s-obs.json",
    "optimizer_transform_col_rules":
        [
            {   "name": "block_size_MiB",
                    "type": "linear", "data_type": "int64", "min": 128, "max": 512, "multiple_of": 128, 
                    "hyperparameter_min": 64.0, "hyperparameter_max": 256.0,
                    "grid_search": 3},
            {   "name": "flush_compute", "type": "boolean", "grid_search": [False], "fixed_value": False},
            {   "name": "io_file_buffer_size", 
                    "type": "log", "data_type": "int64", "min": 4096, "max": 16*1024*1024, 
                    "base": 2, "exponent_multiple_of": 1, "multiple_of": 4096, 
                    "grid_search": [128*1024], "fixed_value": 128*1024},
            {   "name": "map_memory_MB",  # Used for all but only significant for sort
                    "type": "linear", "data_type": "int64", "min": 5*1024, "max": 8*1024, "multiple_of": 1024, 
                    "grid_search": 2, "fixed_value": 5*1024},
            {   "name": "map_tasks", # Used by gen only
                    "type": "linear", "data_type": "int64", "min": 12, "max": 4*(39*3-1), 
                    "hyperparameter_min": 40.0, "hyperparameter_max": 80.0,
                    "grid_search": 10},
            {   "name": "reduce_memory_MB", # Used for sort only
                    "type": "linear", "data_type": "int64", "min": 5*1024, "max": 8*1024, "multiple_of": 1024, 
                    "grid_search": 3},
            {   "name": "reduce_tasks",     # Used for sort only; affects validate
                    "type": "linear", "data_type": "int64", "min": 30, "max": 300, 
                    "hyperparameter_min": 40.0, "hyperparameter_max": 80.0,
                    "grid_search": 10},
            {   "name": "sort_factor", 
                    "type": "log", "data_type": "int64", "min": 10, "max": 1000, 
                    "base": 10, "exponent_multiple_of": 0.5,
                    "hyperparameter_min": 0.5, "hyperparameter_max": 2.0,
                    "grid_search": 5},
            {   "name": "sort_MiB",
                    "type": "linear", "data_type": "int64", "min": 768, "max": 3*1024, "multiple_of": 256, 
                    "grid_search": [768], "fixed_value": 768},
            {   "name": "transparent_hugepage_enabled", "type": "boolean", "fixed_value": False},
        ],
    "optimizer_to_minimize": "elapsed_sec",
    "optimizer_test": "tpcxhs",
    "optimizer_uuid": "a83e02cf-9cad-4f72-96b9-8778ee6eb743",   # Generate with: python -c 'import uuid; print(uuid.uuid4())'
    "reduce_max_attempts": 1,
    "result_filename": "../data/mapred_results/json/%(cluster_name)s_%(storage_cluster_name)s_%(test)s_%(timestamp)s_%(test_uuid)s.json.bz2",
    "test": "optimizer",
    }

test_list.append(config)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
