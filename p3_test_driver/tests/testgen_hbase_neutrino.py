#!/usr/bin/env python

from __future__ import print_function
import json
import sys
import glob

test_list = []

for repeat in range(0,1):
  for num_compute_nodes in [8]:
    for record_count in [num_compute_nodes * 250*1000*1000]:   # num_compute_nodes * 250*1000*1000

              hbase_splits = 10 * num_compute_nodes

              common_config = {
                  "column_family": "family",
                  "flush_compute": False,
                  "hbase_splits": hbase_splits,
                  "kill_all_yarn_jobs": False,
                  "max_test_attempts": 1,
                  "measurement_type": "",
                  "num_compute_nodes": num_compute_nodes,
                  "record_as_test": "hbase",
                  "record_count": record_count,
                  "result_filename": "../data/hbase_results/json/%(cluster_name)s_%(storage_cluster_name)s_%(record_as_test)s_%(ycsb_command)s_%(test_uuid)s.json.bz2",
                  "table_name": "usertable_%d" % record_count,
                  "test": "simple_hadoop",
                  }

              # Load data
              for repeat_load in range(0,1):
                for workload in ['workloada']:
                  for threads in [50]:

                              load_config = common_config.copy()
                              load_config.update({
                                "command_template":
                                  # Drop table if it exists. Ignore errors.
                                  "echo \"disable '%(table_name)s'; drop '%(table_name)s';\" | hbase shell -n "
                                  # Create table with YCSB split.
                                  "; echo \"create '%(table_name)s', '%(column_family)s', "
                                  "{SPLITS => (1..%(hbase_splits)d).map {|i| \\\"user#{1000+i*(9999-1000)/%(hbase_splits)d}\\\"}"
                                  ", MAX_FILESIZE => 1024**4}\" "
                                  "| hbase shell -n && "
                                  # Run YCSB load.
                                  "export JAVA_HOME=%(ycsb_java_home)s ; "
                                  "../ycsb/bin/ycsb "
                                  "%(ycsb_command)s "
                                  "hbase10 "
                                  "-P ../ycsb/workloads/%(workload)s "
                                  "-p table=%(table_name)s "
                                  "-p columnfamily=%(column_family)s "
                                  "-p recordcount=%(record_count)d "
                                  "-p maxexecutiontime=%(max_execution_time_sec)d "
                                  "-threads %(threads)d "
                                  "-s "
                                  "-jvm-args=-Xmx%(ycsb_heap_MB)dm "
                                  "",
                                "max_execution_time_sec": 0,
                                "threads": threads,
                                "workload": workload,
                                "ycsb_command": "load",
                                })
                              test_list.append(load_config)

              # Run transactions
              for repeat_run in range(0,1):
                for workload in ['workloada','workloadb','workloadc']:  # See https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads
                  for max_execution_time_sec in [60*60]:
                    for target_operations_per_sec in [0,1000,100]:  # 0 means unthrottled

                              if target_operations_per_sec == 0:
                                # When unthrottled, use lots of threads.
                                threads = 62
                              else:
                                # Ops/sec/thread is calculated as an integer in YCSB so we must ensure that the number of threads
                                # will allow for an integer value. 
                                # Also, each thread has it's own throttle so if there many more threads than CPUs, actual throughput will be
                                # less than the target.
                                threads = min(target_operations_per_sec, 50)

                              run_config = common_config.copy()
                              run_config.update({
                                "command_template":
                                  "export JAVA_HOME=%(ycsb_java_home)s ; "
                                  "../ycsb/bin/ycsb "
                                  "%(ycsb_command)s "
                                  "hbase10 "
                                  "-P ../ycsb/workloads/%(workload)s "
                                  "-p table=%(table_name)s "
                                  "-p columnfamily=%(column_family)s "
                                  "-p recordcount=%(record_count)d "
                                  "-p operationcount=%(operation_count)d "
                                  "-p maxexecutiontime=%(max_execution_time_sec)d "
                                  "-threads %(threads)d "
                                  "-target %(target_operations_per_sec)d "
                                  "-s "
                                  "-jvm-args=-Xmx%(ycsb_heap_MB)dm "
                                  "",
                                "operation_count": 0,   # this will be ignored since we specify max_execution_time_sec
                                "max_execution_time_sec": max_execution_time_sec,
                                "target_operations_per_sec": target_operations_per_sec,
                                "threads": threads,
                                "workload": workload,
                                "ycsb_command": "run",
                                })
                              test_list.append(run_config)

print(json.dumps(test_list, sort_keys=True, indent=4, ensure_ascii=False))
print('Number of tests generated: %d' % len(test_list), file=sys.stderr)
