#!/usr/bin/env python

from __future__ import print_function
import json
import sys

test_list = []

for repeat in range(3):
  for num_compute_nodes in [7]:
    for io_file_buffer_size in [64*1024]:
      for block_size_MiB in [512]:
        for data_size_MB in [1000*1000]:

                          common = {
                            "base_directory": "%(storage_hadoop_uri)s/benchmarks/terasort-%(data_size_MB)0.0f",
                            "block_size_MiB": block_size_MiB,
                            "data_size_MB": data_size_MB,
                            "io_file_buffer_size": io_file_buffer_size,
                            "map_max_attempts": 1,
                            "map_output_compress_codec": "org.apache.hadoop.io.compress.Lz4Codec",
                            "max_test_attempts": 3,
                            "num_compute_nodes": num_compute_nodes,
                            "reduce_max_attempts": 1,
                            "result_filename": "../data/mapred_results/json/%(cluster_name)s_%(storage_cluster_name)s_%(test)s_%(timestamp)s_%(test_uuid)s.json.bz2",
                            }

                          # Teragen
                          for repeat_gen in range(0,1):
                            for map_cores_gen in [1]:
                              for map_memory_MB in [4096]:
                                for map_tasks_per_compute_node in [32,64,128,11]:
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
                              for reduce_tasks_per_compute_node in [11,8,6]:
                                    reduce_tasks = reduce_tasks_per_compute_node * num_compute_nodes
                                    # Terasort
                                    for map_memory_MB in [4096]:
                                      for reduce_memory_MB in [4096]:
                                        for sort_factor in [31,1000]: #31,100,10
                                          for sort_MiB in [1024]:
                                            for map_output_compress_codec in ["org.apache.hadoop.io.compress.Lz4Codec", None]:
                                                t = common.copy()
                                                t.update({
                                                  "test": "terasort",
                                                  "map_memory_MB": map_memory_MB,
                                                  "map_output_compress_codec": map_output_compress_codec,
                                                  "reduce_memory_MB": reduce_memory_MB,
                                                  "reduce_tasks": reduce_tasks,
                                                  "sort_factor": sort_factor,
                                                  "sort_MiB": sort_MiB
                                                  })
                                                test_list.append(t)
                                    # Teravalidate
                                    for repeat_val in range(0,1):
                                      for map_cores_val in [1]:
                                        for map_memory_MB in [4096]:
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
