#!/usr/bin/env python

from __future__ import print_function
import json
import sys

test_list = []

cores_per_compute = 39

for isilon_num_nodes in [3]:
  for repeat in range(3):
    for num_compute_nodes in [5]:
      for block_size_MiB in [512]:
        for isilon_hdfs_server_threads in [None]:   # 7.2 X410 auto = 80; for 8.0, set to None
          for io_file_buffer_size in [128*1024]:
            for isilon_protection_level in ['2d_1n']:
              for isilon_data_access_pattern in ['streaming']:
                for isilon_smart_cache_enabled in [True]:
                  for isilon_flush in [True]:  # True
                    for flush_compute in [True]: # True
                      for transparent_hugepage_enabled in [None]:
                        for data_size_MB in [1000*1000]:

                              common = {
                                  "base_directory": "%(storage_hadoop_uri)s/benchmarks/%(isilon_data_access_pattern)s-%(isilon_protection_level)s/terasort-%(data_size_MB)0.0f",
                                  "block_size_MiB": block_size_MiB,
                                  "data_size_MB": data_size_MB,
                                  "flush_compute": flush_compute,
                                  "io_file_buffer_size": io_file_buffer_size,
                                  "isilon_data_access_pattern": isilon_data_access_pattern,
                                  "isilon_flush": isilon_flush,
                                  "isilon_hdfs_server_threads": isilon_hdfs_server_threads,
                                  "isilon_num_nodes": isilon_num_nodes,
                                  "isilon_protection_level": isilon_protection_level,
                                  "isilon_smart_cache_enabled": isilon_smart_cache_enabled,
                                  "kill_all_yarn_jobs": True,
                                  "map_output_compress_codec": "org.apache.hadoop.io.compress.Lz4Codec",
                                  "max_test_attempts": 3,
                                  "num_compute_nodes": num_compute_nodes,
                                  "result_filename": "../data/mapred_results/json/%(cluster_name)s_%(storage_cluster_name)s_%(test)s_%(timestamp)s_%(test_uuid)s.json.bz2",
                                  "transparent_hugepage_enabled": transparent_hugepage_enabled,
                                  }

                              # Teragen
                              for repeat_gen in range(1):
                                for map_cores in [1]:
                                  for map_memory_MB in [2048]:
                                    #for map_tasks_per_isilon in [48,64]:
                                    for map_tasks_per_compute in [140]:
                                      #map_tasks = map_tasks_per_isilon * isilon_num_nodes - 1  # subtract 1 for app master
                                      map_tasks = map_tasks_per_compute * num_compute_nodes - 1  # subtract 1 for app master
                                      for isilon_hdfs_server_threads_gen in [isilon_hdfs_server_threads]:
                                                    t = common.copy()
                                                    t.update({
                                                        "test": "teragen",
                                                        "isilon_hdfs_server_threads": isilon_hdfs_server_threads_gen,
                                                        "map_cores": map_cores,
                                                        "map_memory_MB": map_memory_MB,                                                        
                                                        "map_tasks": map_tasks,
                                                        })
                                                    test_list.append(t)

                              # Terasort + Teravalidate
                              for repeat_sort_val in range(1):
                                for reduce_tasks_per_compute in [140]:
                                        reduce_tasks = reduce_tasks_per_compute * num_compute_nodes - 1  # subtract 1 for app master
                                        # Terasort
                                        for repeat_sort in range(0,1):
                                          #for map_cores in [1]:
                                            map_cores = 1
                                            for map_memory_MB in [2048]:
                                              for reduce_memory_MB in [4096]:
                                                for sort_factor in [300]:
                                                  for sort_MiB in [768]:
                                                    t = common.copy()
                                                    t.update({
                                                        "test": "terasort",
                                                        "map_cores": map_cores,
                                                        "map_memory_MB": map_memory_MB,
                                                        "reduce_memory_MB": reduce_memory_MB,
                                                        "reduce_tasks": reduce_tasks,
                                                        "sort_factor": sort_factor,
                                                        "sort_MiB": sort_MiB
                                                        })
                                                    test_list.append(t)
                                        # Teravalidate
                                        for repeat_val in range(0,1):
                                          for isilon_flush_val in [isilon_flush]:
                                            for map_cores in [1]:
                                              for map_memory_MB in [2048]:
                                                for isilon_hdfs_server_threads_val in [isilon_hdfs_server_threads]:
                                                    t = common.copy()
                                                    t.update({
                                                        "test": "teravalidate",
                                                        "isilon_flush": isilon_flush_val,
                                                        "isilon_hdfs_server_threads": isilon_hdfs_server_threads_val,
                                                        "map_cores": map_cores,
                                                        "map_memory_MB": map_memory_MB,
                                                        "map_tasks": reduce_tasks,  # map tasks in validate will always equal reduce tasks from terasort
                                                        })
                                                    test_list.append(t)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
