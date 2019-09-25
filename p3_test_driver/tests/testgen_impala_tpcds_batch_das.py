#!/usr/bin/env python

from __future__ import print_function
import json
import sys
import glob

test_list = []

for repeat in range(0,3):
  for flush_compute in [True]:
    for transparent_hugepage_enabled in [False]:
      for sf in [5]:
        for db_format in ['parquet']:
          for stream_count in [1,10]:
            for query_filespec in [
                ['../impala/impala-tpcds-kit/queries/q*.sql'],
                ['../impala/impala-tpcds-kit/queries/ss_max.sql'],
                ]:
                              config = {
                                  "command_timeout_sec": 3600,
                                  "db_format": db_format,
                                  "db_name": "tpcds_%(db_format)s_%(sf)d",
                                  "db_type": "impala",
                                  "flush_compute": flush_compute,
                                  "kill_all_yarn_jobs": False,
                                  "max_test_attempts": 1,
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
