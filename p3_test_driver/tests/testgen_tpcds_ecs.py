#!/usr/bin/env python

from __future__ import print_function
import json
import sys
import glob

test_list = []

# query_filespec = '../hive-testbench/set1-queries-tpcds/query*.sql'
# query_filenames = sorted(glob.glob(query_filespec))

query_names = [27]
query_filenames = ['../hive-testbench/sample-queries-tpcds/query%s.sql' % n for n in query_names]
storage_access_over_wan = True  # True if access to tables is expected to go over the wan

for repeat in range(10):
  for flush_compute in [False]:
    for transparent_hugepage_enabled in [False]:
      for sf in [3002]:
        for db_format in ['bin_partitioned_orc']:   # 'text', 'bin_partitioned_orc'
          for query_filename in query_filenames:
                              config = {
                                  "command_timeout_sec": 7200,
                                  "db_format": db_format,
                                  "db_name": "tpcds_%(db_format)s_%(sf)d",
                                  "db_type": "hive",
                                  "flush_compute": flush_compute,
                                  "hiveconf:hive.execution.engine": "tez",
                                  "kill_all_yarn_jobs": False,
                                  "max_test_attempts": 1,
                                  "query_filename": query_filename,
                                  "result_filename": "../data/tpcds_results/json_hive/%(cluster_name)s_%(storage_cluster_name)s_%(test)s_%(test_uuid)s.json.bz2",
                                  "sf": sf,
                                  "storage_access_over_wan": storage_access_over_wan,
                                  "test": "sqlquery",
                                  "transparent_hugepage_enabled": transparent_hugepage_enabled,
                                  }
                              test_list.append(config)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
