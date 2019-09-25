#!/usr/bin/env python

from __future__ import print_function
import json
import sys

test_list = []

for repeat in range(0,3):
  for num_compute_nodes in [None]:
    for io_file_buffer_size in [128*1024]:
      for data_size_MB in [10,10*1000,100*1000,1000*1000]:
        for map_tasks in [64]:
          for map_cores in [1]:
            for map_memory_MB in [5743]:

                          common = {
                            "base_directory": "%(storage_hadoop_uri)s/benchmarks/dfsio-%(map_tasks)d-%(data_size_MB)d",
                            "data_size_MB": data_size_MB,
                            "io_file_buffer_size": io_file_buffer_size,
                            "map_cores": map_cores,
                            "map_max_attempts": 1,
                            "map_memory_MB": map_memory_MB,
                            "map_tasks": map_tasks,
                            "max_test_attempts": 3,
                            "num_compute_nodes": num_compute_nodes,
                            "reduce_max_attempts": 1,
                            }

                          # Write
                          for repeat_write in range(0,1):
                                                t = common.copy()
                                                t.update({
                                                  "test": "write",
                                                  })
                                                test_list.append(t)

                          # Read
                          for repeat_read in range(0,1):
                                                t = common.copy()
                                                t.update({
                                                  "test": "read",
                                                  })
                                                test_list.append(t)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
