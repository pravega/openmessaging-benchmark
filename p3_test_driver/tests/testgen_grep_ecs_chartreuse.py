#!/usr/bin/env python

from __future__ import print_function
import json
import sys

test_list = []

for repeat in range(1):
  for num_compute_nodes in [7]:
    for io_file_buffer_size in [64*1024]:
        for data_size_MB in [1000*1000]:

                          common = {
                            "base_directory": "%(storage_hadoop_uri)s/benchmarks/terasort-%(data_size_MB)0.0f",
                            "data_size_MB": data_size_MB,
                            "io_file_buffer_size": io_file_buffer_size,
                            "map_max_attempts": 2,
                            "map_output_compress_codec": "org.apache.hadoop.io.compress.Lz4Codec",
                            "max_test_attempts": 2,
                            "num_compute_nodes": num_compute_nodes,
                            "reduce_max_attempts": 2,
                            "result_filename": "../data/mapred_results/json/%(cluster_name)s_%(storage_cluster_name)s_%(test)s_%(timestamp)s_%(test_uuid)s.json.bz2",
                            }

                          # Teragen
                          for repeat_gen in range(0):
                            for map_cores_gen in [1]:
                              for map_memory_MB in [4096]:
                                for map_tasks_per_compute_node in [11]:
                                                t = common.copy()
                                                t.update({
                                                  "test": "teragen",
                                                  "map_cores": map_cores_gen,
                                                  "map_memory_MB": map_memory_MB,
                                                  "map_tasks": map_tasks_per_compute_node * num_compute_nodes - 1,
                                                  })
                                                test_list.append(t)

                          # Grep
                          for repeat_grep in range(3):
                            for map_cores_grep in [1]:
                              for map_memory_MB in [4096]:
                                for io_file_buffer_size_grep in [io_file_buffer_size]:
                                      t = common.copy()
                                      t.update({
                                        "test": "grep",
                                        "io_file_buffer_size": io_file_buffer_size_grep,
                                        "map_cores": map_cores_grep,
                                        "map_memory_MB": map_memory_MB,
                                        })
                                      test_list.append(t)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
