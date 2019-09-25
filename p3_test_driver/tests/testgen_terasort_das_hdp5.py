#!/usr/bin/env python

from __future__ import print_function
import json
import sys

test_list = []

yarn_nodemanager_memory_MiB = 229376
app_master_memory_MB  = 1024
mem_per_compute_node_MB = yarn_nodemanager_memory_MiB - app_master_memory_MB

for repeat in range(0,3):
  for num_compute_nodes in [4]:
    for io_file_buffer_size in [64*1024]:
      for block_size_MiB in [512]:
        for data_size_MB in [1000*1000]:
          for nr_requests in [128]:  # CentOS 6.7/7.2 default is 128

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
                            "/sys/block/sd*/queue/nr_requests": nr_requests,
                            }

                          # Teragen
                          for repeat_gen in range(0,1):
                            for map_cores_gen in [1]:
                              for map_memory_MB in [2048]:
                                for map_tasks_per_compute_node in [128]:
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
                            # for concurrent_reduce_tasks_per_compute_node in [12]:
                            #   for reduce_tasks_per_compute_node in [2*concurrent_reduce_tasks_per_compute_node]:
                            #         reduce_tasks = reduce_tasks_per_compute_node * num_compute_nodes
                            #         reduce_memory_MB = mem_per_compute_node_MB / concurrent_reduce_tasks_per_compute_node
                            for reduce_tasks_per_compute_node in [39]:
                                    reduce_tasks = int(reduce_tasks_per_compute_node * num_compute_nodes - 1)
                                    # Terasort
                                    for map_memory_MB in [4096]: # 5881
                                      for reduce_memory_MB in [4096]:
                                        for sort_factor in [31]: #31,100,10
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
