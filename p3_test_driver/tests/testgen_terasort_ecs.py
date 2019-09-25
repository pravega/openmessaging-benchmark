#!/usr/bin/env python

from __future__ import print_function
import json
import sys

test_list = []

yarn_nodemanager_memory_MiB = 112*1024
app_master_memory_MB  = 1024
mem_per_compute_node_MB = yarn_nodemanager_memory_MiB - app_master_memory_MB

for repeat in range(0,3):
  for num_compute_nodes in [6]:
    for io_file_buffer_size in [128*1024]:
      for data_size_MB in [1000*1000]:

                          common = {
                            "base_directory": "%(storage_hadoop_uri)s/benchmarks/terasort-%(data_size_MB)0.0f",
                            "data_size_MB": data_size_MB,
                            "io_file_buffer_size": io_file_buffer_size,
                            "map_max_attempts": 1,
                            "map_output_compress_codec": "org.apache.hadoop.io.compress.Lz4Codec",
                            "max_test_attempts": 2,
                            "num_compute_nodes": num_compute_nodes,
                            "reduce_max_attempts": 1,
                            "result_filename": "../data/mapred_results/json/%(cluster_name)s_%(storage_cluster_name)s_%(test)s_%(timestamp)s_%(test_uuid)s.json.bz2",
                            }

                          # Teragen
                          for repeat_gen in range(0,1):
                            for map_cores_gen in [1]:
                              for map_memory_MB in [2048]:
                                for map_tasks_per_compute_node in [20]:
                                                t = common.copy()
                                                t.update({
                                                  "test": "teragen",
                                                  "map_cores": map_cores_gen,
                                                  "map_memory_MB": map_memory_MB,
                                                  "map_tasks": map_tasks_per_compute_node * num_compute_nodes - 1,
                                                  })
                                                test_list.append(t)

                          # Terasort + Teravalidate
                          for repeat_sort in range(0,1):
                            for concurrent_reduce_tasks_per_compute_node in [14]:
                              for reduce_tasks_per_compute_node in [2*concurrent_reduce_tasks_per_compute_node]:
                                    reduce_tasks = reduce_tasks_per_compute_node * num_compute_nodes
                                    reduce_memory_MB = mem_per_compute_node_MB / concurrent_reduce_tasks_per_compute_node
                                    # Terasort
                                    for map_memory_MB in [3584]:
                                      for sort_factor in [31]:
                                        for sort_MiB in [1024]:
                                                t = common.copy()
                                                t.update({
                                                  "test": "terasort",
                                                  "map_memory_MB": map_memory_MB,
                                                  "reduce_memory_MB": reduce_memory_MB,
                                                  "reduce_tasks": reduce_tasks,
                                                  "sort_factor": sort_factor,
                                                  "sort_MiB": sort_MiB
                                                  })
                                                test_list.append(t)
                                    # Teravalidate
                                    for repeat_val in range(0,1):
                                      for map_cores_val in [1]:
                                        for map_memory_MB in [2048]:
                                          for io_file_buffer_size_val in [io_file_buffer_size]:
                                                t = common.copy()
                                                t.update({
                                                  "test": "teravalidate",
                                                  "io_file_buffer_size": io_file_buffer_size_val,
                                                  "map_cores": map_cores_val,
                                                  "map_memory_MB": map_memory_MB,
                                                  "map_tasks": reduce_tasks,  # map tasks in validate will always equal reduce tasks from terasort
                                                  })
                                                test_list.append(t)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
