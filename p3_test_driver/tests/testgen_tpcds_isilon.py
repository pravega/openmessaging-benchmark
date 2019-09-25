#!/usr/bin/env python

from __future__ import print_function
import json
import sys
import glob

test_list = []

query_filespec = '../hive-testbench/set1-queries-tpcds/query*.sql'
query_filenames = sorted(glob.glob(query_filespec))

for isilon_num_nodes in [4]:
  for repeat in range(0,1):
    for num_compute_nodes in [None]:
      for block_size_MiB in [512]:
        # for isilon_hdfs_server_threads in [256]:   # X410 auto = 80, this is the best for preview-4
          for io_file_buffer_size in [128*1024]:
            for isilon_protection_level in ['2d_1n']:
              for isilon_data_access_pattern in ['concurrency']:
                for isilon_smart_cache_enabled in [True]:
                  for isilon_flush in [False]:
                    for flush_compute in [False]:
                      for transparent_hugepage_enabled in [False]:
                        for sf in [1000]:
                          for query_filename in query_filenames:
                              config = {
                                  "block_size_MiB": block_size_MiB,
                                  "command_timeout_sec": 7200,
                                  "db_name": "tpcds_bin_partitioned_orc_%(sf)d",
                                  "db_type": "hive",
                                  "flush_compute": flush_compute,
                                  "io_file_buffer_size": io_file_buffer_size,
                                  # "hive_init_file": "../hive-testbench/sample-queries-tpcds/testbench.settings",
                                  "isilon_data_access_pattern": isilon_data_access_pattern,
                                  "isilon_flush": isilon_flush,
                                  # "isilon_hdfs_server_threads": isilon_hdfs_server_threads,
                                  "isilon_num_nodes": isilon_num_nodes,
                                  "isilon_protection_level": isilon_protection_level,
                                  "isilon_smart_cache_enabled": isilon_smart_cache_enabled,
                                  "kill_all_yarn_jobs": False,
                                  "max_test_attempts": 1,
                                  "num_compute_nodes": num_compute_nodes,
                                  "query_filename": query_filename,
                                  "result_filename": "../data/tpcds_results/json_hive/%(cluster_name)s_%(storage_cluster_name)s_%(test)s_%(test_uuid)s.json.bz2",
                                  "sf": sf,
                                  "test": "sqlquery",
                                  "transparent_hugepage_enabled": transparent_hugepage_enabled,
                                  }
                              test_list.append(config)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
