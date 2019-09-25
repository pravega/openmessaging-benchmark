#!/usr/bin/env python

from __future__ import print_function
import json
import sys

test_list = []

yarn_nodemanager_memory_MiB = 112*1024
app_master_memory_MB  = 1024
mem_per_compute_node_MB = yarn_nodemanager_memory_MiB - app_master_memory_MB

for repeat in range(3):
  for num_compute_nodes in [5]:
    for io_file_buffer_size in [64*1024]:
      for block_size_MiB in [512]:
        for data_size_MB in [1000*1000]:
          for nr_requests in [128]:  # CentOS 6.7 default is 128

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
                          for repeat_gen in range(1):
                            for map_cores in [1]:
                              for map_memory_MB in [2048]:
                                #for map_tasks_per_isilon in [48,64]:
                                for map_tasks_per_compute in [128]:
                                  #map_tasks = map_tasks_per_isilon * isilon_num_nodes - 1  # subtract 1 for app master
                                  map_tasks = map_tasks_per_compute * num_compute_nodes - 1  # subtract 1 for app master
                                  t = common.copy()
                                  t.update({
                                      "test": "teragen",
                                      "map_cores": map_cores,
                                      "map_memory_MB": map_memory_MB,                                                        
                                      "map_tasks": map_tasks,
                                      })
                                  test_list.append(t)

                          # Terasort + Teravalidate
                          for repeat_sort_val in range(1):
                                for reduce_tasks_per_compute in [128]:
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
                                            for map_cores in [1]:
                                              for map_memory_MB in [2048]:
                                                    t = common.copy()
                                                    t.update({
                                                        "test": "teravalidate",
                                                        "map_cores": map_cores,
                                                        "map_memory_MB": map_memory_MB,
                                                        "map_tasks": reduce_tasks,  # map tasks in validate will always equal reduce tasks from terasort
                                                        })
                                                    test_list.append(t)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
