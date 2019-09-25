#!/usr/bin/env python

from __future__ import print_function
import json
import sys
import glob

test_list = []

for isilon_num_nodes in [6]:
  for repeat in range(0,3):
    for num_compute_nodes in [6]:
      for block_size_MiB in [512]:
        for isilon_hdfs_server_threads in [80]:   # X410 auto = 80
          for isilon_protection_level in ['2d_1n']:
            for isilon_data_access_pattern in ['concurrency']:
              for flush_compute in [True]:
                for transparent_hugepage_enabled in [False]:
                  for sf in [15000]:
                    for db_format in ['parquet']:
                      for stream_count in [1,10]:

                        query_filespecs = [['../impala/impala-tpcds-kit/queries/q*.sql']]
                        if stream_count == 1:
                          query_filespecs.extend([['../impala/impala-tpcds-kit/queries/ss_max.sql']])
                        for query_filespec in query_filespecs:

                                            if isilon_data_access_pattern == 'concurrency' and isilon_protection_level == '2d_1n':
                                                db_name = "tpcds_%(db_format)s_%(sf)d"
                                            else:
                                                db_name = "tpcds_%(db_format)s_%(sf)d_%(isilon_data_access_pattern)s_%(isilon_protection_level)s"

                                            config = {
                                                "block_size_MiB": block_size_MiB,
                                                "command_timeout_sec": 3600,                                                
                                                "db_format": db_format,
                                                "db_name": db_name,
                                                "db_type": "impala",
                                                "flush_compute": flush_compute,
                                                "kill_all_yarn_jobs": False,
                                                "isilon_data_access_pattern": isilon_data_access_pattern,
                                                "isilon_flush": True,
                                                "isilon_hdfs_server_threads": isilon_hdfs_server_threads,
                                                "isilon_num_nodes": isilon_num_nodes,
                                                "isilon_protection_level": isilon_protection_level,
                                                "isilon_smart_cache_enabled": True,
                                                "max_test_attempts": 1,
                                                "num_compute_nodes": num_compute_nodes,
                                                "profile_query": True,
                                                "query_filespec": query_filespec,
                                                "queries_per_stream": 0,
                                                "random_seed": 31415,
                                                "result_filename": "../data/tpcds_results/json_impala/%(cluster_name)s_%(storage_cluster_name)s_%(record_type)s_%(record_uuid)s.json.bz2",
                                                "sf": sf,
                                                "stream_count": stream_count,
                                                "test": "sqlbatch",
                                                "transparent_hugepage_enabled": transparent_hugepage_enabled,
                                                }
                                            test_list.append(config)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
