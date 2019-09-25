#!/usr/bin/env python

from __future__ import print_function
import json
import sys
import glob

test_list = []

config = {
    "command_timeout_sec": 7200,
    "db_format": 'bin_partitioned_orc',
    "db_name": "tpcds_%(db_format)s_%(sf)d",
    "db_type": "hive",
    "kill_all_yarn_jobs": True,
    "max_test_attempts": 1,
    "optimizer_extra_points_file": "../data/optimizer/%(optimizer_uuid)s-extra.json",
    "optimizer_grid_search": True,
    "optimizer_max_moe_iterations": 10000,
    "optimizer_moe_rest_host": "hop-claudio-ubuntu-desktop",
    "optimizer_min_sample_size": 2,
    "optimizer_observations_file": "../data/optimizer/%(optimizer_uuid)s-obs.json",
    "optimizer_transform_col_rules":
        [
            # See https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties
            # See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+JoinOptimization
            {   "name": "flush_compute", "type": "boolean", "grid_search": [False], "fixed_value": False},
            {   "name": "hiveconf:hive.auto.convert.join.noconditionaltask.size", 
                    "type": "log", "data_type": "int64", "min": 10e6, "max": 8e9, 
                    "base": 2, "exponent_multiple_of": 0.5, "multiple_of": 1024**2, 
                    "grid_search": [759169024]},
            {   "name": "hiveconf:hive.exec.reducers.bytes.per.reducer", 
                    "type": "log", "data_type": "int64", "min": 1e8, "max": 1e10, 
                    "base": 10, "exponent_multiple_of": 0.5, "value_if_missing": 1e9,
                    "grid_search": [1e9]},
            {   "name": "hiveconf:hive.auto.convert.sortmerge.join", "type": "boolean", "grid_search": [False], "Xfixed_value": False},
            #{   "name": "hiveconf:hive.tez.dynamic.partition.pruning", "type": "boolean", "grid_search": [True]},
            {   "name": "hiveconf:hive.optimize.bucketmapjoin.sortedmerge", "type": "boolean", "grid_search": [False], "Xfixed_value": False},
            {   "name": "hiveconf:hive.optimize.index.filter", "type": "boolean", "grid_search": [True], "Xfixed_value": True},
            {   "name": "hiveconf:hive.exec.reducers.max", 
                    "type": "linear", "data_type": "int64", "min": 1, "max": 2000, "multiple_of": 1, "value_if_missing": 999,
                    "grid_search": [999]},
            {   "name": "hiveconf:hive.optimize.reducededuplication.min.reducer", 
                    "type": "linear", "data_type": "int64", "min": 0, "max": 10, "multiple_of": 1, 
                    "grid_search": [1], "Xfixed_value": 1},
            {   "name": "hiveconf:hive.smbjoin.cache.rows",
                    "type": "log", "data_type": "int64", "min": 1e2, "max": 1e6, "value_if_missing": 1e4,
                    "base": 10, "exponent_multiple_of": 0.5, 
                    "grid_search": [1e4], "Xfixed_value": 1e4},
            {   "name": "hiveconf:hive.tez.container.size",
                    "type": "linear", "data_type": "int64", "min": 1024, "max": 32*1024, "multiple_of": 1024, 
                    "grid_search": [6*1024]},
            {   "name": "hiveconf:hive.stats.fetch.column.stats", "type": "boolean", "value_if_missing": False, "grid_search": [False], "Xfixed_value": False},
            {   "name": "hiveconf:hive.compute.query.using.stats", "type": "boolean", "grid_search": [False], "Xfixed_value": False},
            {   "name": "hiveconf:hive.vectorized.execution.enabled", "type": "boolean", "grid_search": [False], "Xfixed_value": True},
            {   "name": "hiveconf:hive.vectorized.execution.reduce.enabled", "type": "boolean", "value_if_missing": True, "grid_search": [True], "Xfixed_value": True},
            {   "name": "hiveconf:hive.vectorized.execution.reduce.groupby.enabled", "type": "boolean", "value_if_missing": True, "grid_search": [True], "Xfixed_value": True},
            {   "name": "java_opts_xmx_ratio", 
                    "type": "linear", "min": 0.5, "max": 1.0, "multiple_of": 0.05,
                    "grid_search": [0.6], "Xfixed_value": 0.6},
            {   "name": "transparent_hugepage_enabled", "type": "boolean", "grid_search": [True], "Xfixed_value": True},
        ],
    "optimizer_to_minimize": "query_elapsed_sec",
    "optimizer_to_minimize_domain": [0.0, 7200.0],
    "optimizer_test": "sqlquery",
    "optimizer_uuid": "0d5f5b5b-057e-404c-977b-19c704d1ca08",   # Generate with: python -c 'import uuid; print(uuid.uuid4())'
    "query_filename": '../hive-testbench/set1-hive13-queries-tpcds/query60.sql',
    "result_filename": "../data/tpcds_results/json_hive/%(cluster_name)s_%(storage_cluster_name)s_%(test)s_%(test_uuid)s.json.bz2",
    "sf": 1000,
    "test": "optimizer",
    }

test_list.append(config)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
