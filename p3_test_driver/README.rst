
==============
P3 Test Driver
==============

General Purpose Pluggable System Test Driver

Claudio Fahey (claudio.fahey@emc.com)

********
Overview
********

The P3 Test Driver is designed to run a variety of benchmarks using an easily expandble plug-in system.

It currently runs the following benchmarks:

- TestDFSIO
- Teragen, Terasort, Teravalidate
- TPC-DS (Hive, HAWQ, Impala)
- TPCx-HS
- YCSB (HBase)

Although it can perform tests on any storage system, it comes with plugins that provide special support
for EMC Isilon Scale-out NAS and EMC Elastic Cloud Storage (ECS) platforms.

It is intended to be relatively lightweight in that it only requires the Python runtime and several Python libraries 
to be installed on a single "driver" node.
In particular, it doesn't require a database server or any infrastructure other than password-less SSH
access to the nodes under test. 
The output of the P3 Test Driver is a compressed JSON file consisting of key-value pairs that define both
the parameters of the test (inputs), and the result of the test (outputs). These output files can be analyzed 
using the related tool called Test Results Analyzer or any other tool that can read JSON files.

A common factor in all benchmarks is that there are usually many different tunable parameters that can affect
the benchmark results. For instance, in a MapReduce job, changing the number of reducers will impact the performance. 
Normally, there is an optimal number of reducers for a particular job and values far away from this optimal value will 
result in reduced performance.

The P3 Test Driver can run in two modes (again, these modes are defined by a replacable plugin).
The simplist mode is simply to run tests using parameters that are completely defined by the user.
For instance, the user may specify that they want to run Terasort with 10, 30, 100, and 300 reducers in order to
see which one is fasteset. 
This can be extended along multiple dimensions to perform a simple grid search.
For instance, if the user also wanted to test with different
values of the reducer memory (i.e. 2048, 4096, and 8192 MB), then a cartesian product of these two
dimensions could be created and every possibility would be executed. 
In this example, there would be 4*3 = 12 different sets of parameters that would be tested. 

As more dimensions (variables) are added, the number of parameter combinations to be tested in such 
a grid search becomes large very quickly. 
To deal with this, the P3 Test Driver has an optimzation mode based on the Metrics Optimization Engine (MOE)
developed at Yelp (see https://github.com/Yelp/moe). 
The goal of this optimization mode is to automatically find the best set of parameters
for the particular benchmark. See the Optimization section below for details.   


*************
Prerequisites
*************

#.  A Linux server will be used run the P3 Test Driver. For Hadoop tests, this server should also have
    the Hadoop client so that it can stop and start jobs.
    The following Linux distributions have been tested.
    - CentOS 6.6, 6.7, 7.2
    - Ubuntu 12.04
  
#.  Times on all compute nodes must be synchronized to within a second, preferably using NTP.
    
#.  The time zone on all systems should be UTC.
    Although not strictly required, this makes troubleshooting distributed systems much simpler.

#.  To collect Linux metrics, nmon version 14g should be installed on all Linux hosts.

*************************
Quick Installation in Lab
*************************

Use this when everything is already installed on a mounted NFS share.

[user\@driver-server p3_test_driver]#

mkdir -p .ssh
cp /mnt/home/faheyc/ssh/id_rsa* ~/.ssh/
chmod go-r .ssh/id_rsa*

Create file ~/.ssh/config with following contents:
Host *
   StrictHostKeyChecking no
   UserKnownHostsFile /dev/null

export PATH=/mnt/home/faheyc/anaconda/bin:$PATH


***********************************
Where to Install the P3 Test Driver
***********************************

The P3 Test Driver itself will run on a driver node. For Hadoop tests, this must be a node
that has the Hadoop client installed. If testing Hive, Impala, or HBase, it should have
those clients installed as well. For small clusters, a Hadoop master node can be used for this.

The user account to use on the driver node can be any user with the appropriate permissions to
run the test. In a lab environment, the root user can be used.

When logging in to the driver node as this user, you should ensure that your session will not be interrupted
by a disconnected VPN or WAN link. It is recommended to use a console that is on the same LAN as the 
driver node.


*****************
Password-less SSH
*****************

Password-less SSH is required from the user and server running the P3 Test Driver to all other servers involved in the
test. This can be configured in a variety of ways. 
The easiest method is to use configure-ssh.py from 
https://github.com/claudiofahey/devops-scripts/blob/master/configure-ssh.py.

.. parsed-literal::

  [root\@driver-server p3_test_driver]# rpm -i centos6/sshpass*.rpm
  [user\@driver-server p3_test_driver]# ssh-keygen -t rsa -b 4096
  [user\@driver-server p3_test_driver]# configure-ssh.py -u root -p mypassword worker1 worker2 worker3

Alternatively:

.. parsed-literal::

  [user\@driver-server p3_test_driver]# for n in {001..010} ; do ./configure-ssh.py -u root -p mypassword node$n.example.com ; done


************
Installation
************

Although the Python runtime that is installed by default in Linux can often be used, it is
significantly easier to use the Python runtime provided by Anaconda as it makes
it very easy to install all required packages and it will not interfere with any other
applications that use Python.


Basic Installation using Anaconda
---------------------------------

Perform these steps on the Linux server that will run the P3 Test Driver.
The commands below are for CentOS 6.6 but this is expected to work under Ubuntu or other
distributions with the appropriate commands.

#. Using your browser, download Anaconda for Linux 64-bit, Python 2.7 from https://www.continuum.io/downloads. 
#. sudo bash ./Anaconda2-2.4.1-Linux-x86_64.sh -b -p /opt/anaconda
#. export PATH=/opt/anaconda/bin:$PATH
#. conda update anaconda
#. conda install pyramid
#. pip install yapsy simplejson colander

To configure the current shell instance to use Anaconda Python, set the path
using the command shown below. This must be repeated whenever a new shell instance starts.

.. parsed-literal::

  [user\@driver-server p3_test_driver]# **export PATH=/opt/anaconda/bin:$PATH**


Additional Installation Steps to Enable Optmization
---------------------------------------------------

To use the automatic optimization feature of the P3 Test Driver, MOE must be installed.
If you do not need to use the optimization feature at this time, this section can be skipped.

There are two components that must be installed. 
First, the MOE docker container must be installed. This runs a web service that the P3 Test Driver
will call in order to perform the necessary calculations. When MOE is used to optimize a system
with many historical points (in the hundreds) and/or many dimensions (5 or more), it can be very
CPU intensive. For this reason, the MOE docker container should be installed on a server that it outside of
the system to be tested, preferrably with 32 or more cores.

For more details regarding MOE, see https://github.com/Yelp/moe.

Install Docker
==============

Before installing the MOE Docker container, Docker must be installed.

#. wget -qO- https://get.docker.com/gpg | sudo apt-key add -
#. wget -qO- https://get.docker.com/ | sh

Direct Download of MOE Docker Image
===================================

Use this method if you have a direct Internet connection.

#. sudo docker pull yelpmoe/latest
#. sudo docker run -p 6543:6543 yelpmoe/latest
       
Indirect Download of MOE Docker Image
=====================================

Use this method if the system that you want to install the MOE Docker container does not
have Internet access.

#. other host: sudo docker pull yelpmoe/latest
#. other host: docker save yelpmoe/latest | bzip2 > yelpmoe.tar.bz2
#. bunzip2 < yelpmoe.tar.bz2 | docker load 
#. sudo docker images
#. sudo docker run -p 6543:6543 9bb6643de2e6

Install MOE Client Library
==========================

The MOE client library must be installed on the Linux server that will run the 
P3 Test Driver.

#. yum install git
#. git clone https://github.com/Yelp/MOE
#. cd MOE
#. export PATH=/opt/anaconda/bin:$PATH
#. export MOE_NO_BUILD_CPP=True
#. python setup.py install
#. Test:

   #. python
   #. >>> from moe.easy_interface.simple_endpoint import gp_next_points
   #. >>> import yapsy
   #. >>> exit()


Prepare Hadoop for Benchmarks
-----------------------------

.. parsed-literal::

  [hdfs\@hadoop-master-0 ~]# hdfs dfs -mkdir -p /benchmarks
  [hdfs\@hadoop-master-0 ~]# hdfs dfs -chmod -R 777 /benchmarks

.. parsed-literal::

  [root\@hadoop-master-0 ~]# adduser hduser1
  [hdfs\@hadoop-master-0 ~]# hdfs dfs -mkdir -p /user/hduser1
  [hdfs\@hadoop-master-0 ~]# hdfs dfs -chown hduser1:hduser1 /user/hduser1
  [hdfs\@hadoop-master-0 ~]# hdfs dfs -chmod -R a+rwX /user/hduser1


****************************
Test and Configuration Files
****************************  

The P3 Test Driver runs unattended as a command-line application. A set of JSON files
instructs it run one or more tests.

An example *test* file is:

.. parsed-literal::

  [
      {
          "test": "teragen",
          "data_size_MB": 1000000,
          "block_size_MiB": 512
      },
      {
          "test": "terasort",
          "reduce_tasks": 100
      },
      {
          "test": "teravalidate"
      }
  ]

This test file instructs the P3 Test Driver to run Teragen to create 1 TB of data using a block size of 512 MiB.
Once Teragen completes, it will run Terasort and then Teravalidate.
Additional tests can be added simply by adding to the list (between "[" and "]").
Test parameters can be specified by adding additional key/value pairs.
Values themselves can be simple scalars (as shown in the example) or they can be nested key/value
pairs or any other valid JSON data type.

There are a few parameters that control how the P3 Test Driver framework run tests such as "max_test_attempts".
Additionally, each test defines many other test-specific parameters such as "reduce_tasks".

Most tests will also need parameters that define environment-specific properties such as host names, file paths,
URLs, etc.. These can be specified separately (and repeated) for each test or they can be specified once
in a *configuration* JSON file.

An example *configuration* file is:

.. parsed-literal::

  {
    "mapred_history_host": "hadoop-master-0",
    "mtu": 1500,
    "num_local_disks_per_physical_compute_nodes": 12,
    "status_html": "../data/status/status.html",
    "test_driver_log_filename": "../data/p3_test_driver_logs/driver.log"
  }

There are some parameters that must be defined in the configuration file and not the test file.
One such parameter is "test_driver_log_filename" and defines the path to the log file
that the P3 Test Driver will use. 
All other parameters can be defined either in the configuration file
(for parameters that are completely or mostly common to all tests) or they can be defined
in the test file. For any parameters defined in both configuration files, the value specified in
the test-specific test file will be used.

When tests begin to execute, the P3 Test Driver will internally build a record consisting of the key/value pairs
in the configuration files and test files. Additional key/value pairs will be added by the P3 Test Driver
(e.g. "test_attempt", "test_uuid") and by the test plugin (e.g. "hadoop_command", "utc_begin").
When each test completes, additional key/value pairs will be added (e.g. "elapsed_sec", "exit_code")
and the resulting set of key/value pairs will be written to a compressed JSON file.

Since the records are written as JSON files, there is a lot of flexibility in the data types that
are written. The included test plugins take advantage of this by recording a plethora of
information such as the entire stdout/stderr of the command (including timestamps for each line),
contents of various configuration files as they existed when the test executed, 
a variety of metrics (CPU, disk, network) of all involved hosts. All of this information is in a 
single self-contained JSON file that completely describes the environment, test inputs, and test outputs.
In general, it is better to record too much information than not enough.

For the most part, the configuration and test files are schema-less. 
Any key/value pairs specified for unknown keys are automatically added to the JSON file that is recorded
at the end of the test. This is useful in a variety of situations. For instance, a user may have
performed all previous tests with widget version 1.0 and now they upgraded to widget 2.0. To keep
track of the tests that ran with widget 2.0, simply add a configuration key "widget_version" with the value
"2.0". Of course, when analyzing the results, the user will need to know that a missing key implies widget 1.0.


***********************************
Automatically Generating Test Files
***********************************

A user will often want to run a large number of tests using the P3 Test Driver.
Of course, this can be performed simply by typing out a very large JSON file describing
all of the tests to run. However, a better approach is to programatically create the JSON file
that describes all of the tests.

For an example, see the Python script tests/testgen_terasort_das.py. This will iterate over several
common parameters (block_size_MiB, data_size_MiB). For each set of these common parameters
it will run Teragen using several different
values of map_tasks, then it will run Terasort using several different values of reduce_tasks, and then it
will run Teravalidate. This cycle will repeat for all common parameters and identical tests
can be repeated as desired. This script uses several levels of nested loops in order to run the sequence of jobs in
the most efficient manner possible but taking into account the effect of previous jobs on subsequent
jobs (e.g. the number of reducers during Terasort affects Teravalidate).

When the script tests/teragen_terasort_das.py executes, it doesn't actually run the tests. It simply outputs
the JSON that describes the tests that should run. This JSON can then be fed into the P3 Test Driver to have it
actually execute the tests.


*********************************
P3 Test Driver Command-line Usage
*********************************

+-----------------------------+---------------------------------------------------------------------------------------------------------+
| Parameter Name              | Description                                                                                             |
+=============================+=========================================================================================================+
| --config config.json        | Read global/common key/value pairs from the file config.json. This can be specified multiple times.     |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| --tests tests.json          | Read test-specific key/value pairs from the file tests.json. This can be specified multiple times.      |
|                             | If the parameter is "-", the list of tests will be read from stdin. This is convenient when generating  |
|                             | the tests using a script.                                                                               |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| --skip n                    | Skip the first n tests.                                                                                 |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| --num-tests n               | Run only this number of tests.                                                                          |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| --dump-tests-configs        | Build record key/value pairs from all specified JSON files but not do actually run the tests.           |
+-----------------------------+---------------------------------------------------------------------------------------------------------+


*************************************
Tips for Launching the P3 Test Driver
*************************************

When using the P3 Test Driver on multiple systems, it is convenient to have a configuration file to define
each separate environmental component. For instance, the file my_hadoop_cluster.json can define the properties for
your Hadoop cluster (e.g. mapred_history_host, job_client_jar) while the file my_storage_cluster.json
can define the properties for your storage cluster (storage_host, storage_hadoop_uri). 

Additionally, use a separate testgen.py script to generate each batch of tests. For example, one testgen file
will define a set of Terasort suite jobs while another can define a set of TPC-DS queries.

With configuration components and tests defined in separate files, they can be combined in a variety of ways.

For example:

.. parsed-literal::

  [user\@driver-server p3_test_driver]# **export PATH=/opt/anaconda/bin:$PATH**
  [user\@driver-server p3_test_driver]# **tests/testgen_terasort_das.py | ./p3_test_driver.py \
  --config my_hadoop_cluster.json --config my_storage_cluster.json \
  --tests -**

The previous command-line will run a set of Terasort suite tests on a particular Hadoop cluster with
a particular storage cluster.


**************************************
P3 Test Driver Global Input Parameters
**************************************

These must be specified in the configuration JSON file (--config).

+-----------------------------+---------------------------------------------------------------------------------------------------------+
| Parameter Name              | Description                                                                                             |
+=============================+=========================================================================================================+
| status_html                 | This is the path to the status file. This file can be opened in a browser and will automatically        |
|                             | refresh every few seconds.                                                                              |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| test_driver_log_filename    | This is the path the the log file used by P3 Test Driver.                                               |
+-----------------------------+---------------------------------------------------------------------------------------------------------+


**************************************
P3 Test Driver Common Input Parameters
**************************************

These parameters can be specified in the configuration JSON file (--config) or
the test JSON file (--test). Values specified in the last test file will take precedence.

+-----------------------------+---------------------------------------------------------------------------------------------------------+
| Parameter Name              | Description                                                                                             |
+=============================+=========================================================================================================+
| _COMMON_FILE_CONFIG         | This is a special parameter. When this value is True, the other parameters in this test will be copied  |
|                             | to subsequent tests in this test file.                                                                  |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| max_test_attempts           | If the test fails, it will automatically be repeated up to a maximum of this many attempts.             |
|                             | A value of 1 means the test will execute exactly once even if an error occurs.                          |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| noop                        | (No-Operation) If True, most tests will log diagnostics information but will not actually run.          |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| result_filename             | This is the path to the result JSON file.                                                               |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| sysctl_vm.swappiness        | If set, the kernel parameter vm.swappiness will be set to this value.                                   |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| sysctl_vm.overcommit_ratio  | If set, the kernel parameter vm.overcommit_ratio will be set to this value.                             |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| test                        | The type of test to run. Available values are: write, read, teragen, terasort, teravalidate. Write must |
|                             | precede read. Teragen, terasort, and teravalidate must run in order.                                    |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| test_variant                | "standard" or any other value to indicate a non-standard test.                                          |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| transparent_hugepage_enabled| If true, this kernel setting is set to 'always'                                                         |
+-----------------------------+---------------------------------------------------------------------------------------------------------+


***************************************
P3 Test Driver Common Output Parameters
***************************************

The result JSON file will be written at the completion of each test. It will consists of key/value pairs for each input parameter
as well as the output parameters described below.

+-----------------------------+---------------------------------------------------------------------------------------------------------+
| Key                         | Description                                                                                             |
+=============================+=========================================================================================================+
| TODO                        |                                                                                                         |
+-----------------------------+---------------------------------------------------------------------------------------------------------+


******************************
Hadoop Common Input Parameters
******************************

These parameters can be specified in the configuration JSON file (--config) or
the test JSON file (--test). Values specified in the last test file will take precedence.

+-----------------------------+---------------------------------------------------------------------------------------------------------+
| Parameter Name              | Description                                                                                             |
+=============================+=========================================================================================================+
| app_master_memory_MB        | Memory to allocate to the Application Master.                                                           |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| base_directory              | Hadoop URI for test data. Do not include a trailing "/" character. Supports variable substitution.      |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| block_size_MiB              | HDFS block size to give to the Hadoop command. In general, this only applies to new files.              |
|                             | (dfs.blocksize)
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| buffer_size                  | The buffer size used by TestDFSIO.                                                                      |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| cluster_name                | Name of Hadoop compute cluster.                                                                         |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| collect_text_files_node_    | List of files whose content should be captured in the result file.                                      |
| manager                     |                                                                                                         |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| data_size_MB                | The total size of all files generated.                                                                  |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| examples_jar                | Path to hadoop-mapreduce-examples.jar.                                                                  |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| flush_compute               | If true, disk cache on the compute nodes will be flushed before the test begins.                        |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| hadoop_authentication       | "standard" or "kerberos"                                                                                |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| hadoop_client_host          | FQDN of YARN Resource Manager.                                                                          |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| hadoop_command_env          | Dictionary of environment variables to set when running the Hadoop command.                             |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| hadoop_parameters           | List of additional parameters to give to the Hadoop command.                                            |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| io_file_buffer_size         | Corresponds to the Hadoop parameter io.file.buffer.size.                                                |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| java_opts_xmx_ratio         | The Java maximum heap memory will be this fraction of the YARN container.                               |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| job_client_jar              | Path to hadoop-mapreduce-client-jobclient.jar.                                                          |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| kerberos_keytab             | Path to .keytab file that allows authentication as kerberosPrincipalName (not implemented)              |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| kerberos_principal_name     | Kerberos principal name for running tests (not implemented)                                             |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| kill_all_yarn_jobs          | If true, all YARN jobs will be killed before the test begins.                                           |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| mapred_history_host         | FQDN of the MapReduce History Server.                                                                   |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| mapred_log_collect          | If true, MapReduce task logs will be collected.                                                         |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| mapred_log_dir              | Directory that will contain collected MapReduce task logs.                                              |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| map_cores                   | Number of CPU cores to allocate to each map task. (mapreduce.map.cpu.vcores)                            |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| map_memory_MB               | Memory to allocate to each map task.                                                                    |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| map_output_compress_codec   | Set the value of the Hadoop parameter mapred.map.output.compress.codec.                                 |
|                             | "org.apache.hadoop.io.compress.Lz4Codec" is recommended.                                                |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| max_test_attempts           | Number of times to attempt this test before giving up and moving to the next test.                      |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| map_max_attempts            | Maximum number of attempts for each mapper task. 1 means attempt exactly once.                          |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| map_tasks                   | The number of mappers for the job.                                                                      |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| num_compute_nodes           | The number of compute nodes to use. YARN NodeManagers will be started or stopped to achieve this count. |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| reduce_max_attempts         | Maximum number of attempts for each reducer task. 1 means attempt exactly once.                         |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| reduce_memory_MB            | Memory to allocate to each reduce task.                                                                 |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| reduce_tasks                | The number of reduce tasks. In subsequent teravalidate tests, this will be uesd as the number of        |
|                             | mappers.                                                                                                |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| storage_hadoop_uri          | If the Hadoop URI for this storage system is not the default file system, specify the URL               |
|                             | (without a trailing "/").                                                                               |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| sort_factor                 | Set the value of the Hadoop parameter io.sort.factor.                                                   |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| sort_MiB                    | Set the value of the Hadoop parameter mapreduce.task.io.sort.mb. For best results, make this slightly   |
|                             | larger than your HDFS block size to avoid spills.                                                       |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| terasort_output_replication | Output files will have this many HDFS block replicas. Default is 1.                                     |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| yarn_service_control_method | Set to "yarn-daemon.sh" for HDP. Set to "service" for PHD. (not implemented)                            |
+-----------------------------+---------------------------------------------------------------------------------------------------------+


*************************************************
Test Parameters to Enable MapReduce Debug Logging
*************************************************

To view full debug logs, set the parameters as described below.

+--------------------+----------------------------------------------------------------------------------------------------------------------+
| Parameter Name     | Value                                                                                                                |
+====================+======================================================================================================================+
| hadoop_command_env | This will enable trace logging for the MapReduce driver that launches and monitors the Hadoop Job.                   |
|                    | {"HADOOP_ROOT_LOGGER": "TRACE,console"}                                                                              |
+--------------------+----------------------------------------------------------------------------------------------------------------------+
| hadoop_parameters  | This will enable trace logging for all mapper and reducer tasks.                                                     |
|                    | ["-Dmapreduce.map.log.level=TRACE", "-Dmapreduce.reduce.log.level=TRACE", "-Dyarn.app.mapreduce.am.log.level=TRACE"] |
+--------------------+----------------------------------------------------------------------------------------------------------------------+
| mapred_log_dir     | To download mapper and reducer logs, you must provide a path to store them. They are not included in the             |
|                    | JSON file.                                                                                                           |
|                    | e.g.: "../data/mapred_results/mapredlogs"                                                                            |
+--------------------+----------------------------------------------------------------------------------------------------------------------+


*******************************
Hadoop Common Output Parameters
*******************************

The results JSON file will be written at the completion of each test. It will consists of key/value pairs for each input parameter
as well as the output parameters described below.

+-----------------------------+---------------------------------------------------------------------------------------------------------+
| Key                         | Description                                                                                             |
+=============================+=========================================================================================================+
| TODO                        |                                                                                                         |
+-----------------------------+---------------------------------------------------------------------------------------------------------+


***********************************
EMC Isilon Storage Input Parameters
***********************************

These parameters can be specified in the configuration JSON file (--config) or
the test JSON file (--test). Values specified in the last test file will take precedence.

+-----------------------------+---------------------------------------------------------------------------------------------------------+
| Parameter Name              | Description                                                                                             |
+=============================+=========================================================================================================+
| isilon_flush                | If true, the Isilon cache is flushed prior to the test. **WARNING: This should not be enabled on        |
|                             | production systems!**                                                                                   |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| isilon_hdfs_block_size_mb   | Isilon HDFS block size.                                                                                 |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| isilon_hdfs_log_level       | "INFO", "DEBUG", etc.                                                                                   |
|                             | For OneFS 8.0 or higher, this must parameter must be ommitted or set to null (None in Python).          |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| isilon_hdfs_server_threads  | Before starting the test, the Isilon HDFS daemon will be configured to use this many threads.           |
|                             | Specify "auto" to use the Isilon-specific default.                                                      |
|                             | For OneFS 8.0 or higher, this must parameter must be ommitted or set to null (None in Python).          |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| isilon_host                 | Isilon host IP or DNS name. This will be used to submit SSH and web service commands.                   |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| isilon_node_pool_name       | Name of the Isilon node pool used for HDFS. The number of nodes in this pool will be reduced to match   |
|                             | numIsilonNodes.                                                                                         |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| isilon_num_nodes            | The number of Isilon nodes to use. Excess Isilon nodes will be Smartfailed.                             |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| _isilon_password            | Password to authenticate to the Isilon web service.                                                     |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| isilon_ssd_strategy         | Informative only. Suggested values are "metadata", "metadata-write", "l3".                              |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| isilon_user                 | User to SSH into Isilon as.                                                                             |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| storage_cluster_name        | Name that describes this storage system.                                                                |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| storage_type                | "isilon"                                                                                                |
+-----------------------------+---------------------------------------------------------------------------------------------------------+


***********************************
EMC ECS Storage Input Parameters
***********************************

These parameters can be specified in the configuration JSON file (--config) or
the test JSON file (--test). Values specified in the last test file will take precedence.

+-----------------------------+---------------------------------------------------------------------------------------------------------+
| Parameter Name              | Description                                                                                             |
+=============================+=========================================================================================================+
| storage_cluster_name        | Name that describes this storage system.                                                                |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| storage_flush               | Not implemented.                                                                                        |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| storage_host                | IP or FQDN of one of the storage node                                                                   |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| storage_host_names          | List of IP or FQDN of all storage nodes.                                                                |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| storage_num_nodes           | Number of storage nodes.                                                                                |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| storage_type                | "ecs"                                                                                                   |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| storage_user                | User to SSH into the storage nodes as.                                                                  |
+-----------------------------+---------------------------------------------------------------------------------------------------------+


*********************
Variable Substitution
*********************

Some input parameters support variable substitution using any other input parameter.

For example:

  %(storage_hadoop_uri)s/benchmarks/terasort-%(data_size_MB)0.0f

This value for base_directory will use the storage_hadoop_uri and data_size_MB parameters to build the base directory.
Refer to the Python "%" operator for formatting options.


*******
Metrics
*******

The P3 Test Driver has the capability of collecting various metrics from all related systems. 
This is done in a generic way by running one or more commands, collecting the text output,
and storing the text output in the output JSON file.

For example, the start command below will SSH into a Linux host and run nmon to begin collecting performance metrics.
Multiple instances of the command will run in parallel, one for each related host.

.. parsed-literal::

  ssh root@host1.example.com "pkill -USR2 nmon ; rm -f /tmp/nmon.csv ; TZ=UTC nmon -F /tmp/nmon.csv -T -s 5 -c 1000000"

The desired test will then run. When complete, the following stop command will execute to stop nmon and output the results for 
collection by the P3 Test Driver.

.. parsed-literal::

  ssh root@host1.example.com "pkill -USR2 nmon ; cat /tmp/nmon.csv"

To direct the P3 Test Driver to run these commands on all Hadoop NodeManager hosts, the following example configuration parameter
can be specified.

.. parsed-literal::

  "metrics_group:compute": {
      "host_names_key": "compute_node_host_names",
      "agents": {
          "nmon:compute:%(hostname)s": {
              "start_cmd": "ssh root@%(hostname)s \"pkill -USR2 nmon ; rm -f /tmp/nmon.csv ; TZ=UTC nmon -F /tmp/nmon.csv -T -s 5 -c 1000000\"",
              "stop_cmd": "ssh root@%(hostname)s \"pkill -USR2 nmon ; cat /tmp/nmon.csv\""
          }
      }
  }

A key with a prefix of "metrics_group:" indicates a group of hosts on which to execute commands to collect metrics.
For instance, "metrics_group:compute" refers to the Linux compute hosts of a Hadoop cluster and "metrics_group:master"
refers to the Linux master hosts of a Hadoop cluster.

The value of a metrics_group is a dictionary (hash) containing host_names_key and agents. 
host_names_key must be the name of a key that contains a list of host names that are members of the group.
If host_names_key is "compute_node_host_names", then this will automatically refer to all hosts that are
actively running the Hadoop NodeManager service.
The agents key contains a dictionary (hash) whose key is the agent ID (a string uniquely identifying the host and metrics command)
and whose value contains the start and optional stop command.

As another example, to collect statistics from an EMC Isilon cluster, the following example configuration parameter
can be specified.

.. parsed-literal::

  "metrics_group:storage": {
      "agents": {
          "isi_statistics_system": {
              "start_cmd": "ssh %(isilon_user)s@%(isilon_host)s isi statistics system --nodes --timestamp --csv -i5"
          },
          "isi_statistics_drive": {
              "start_cmd": "ssh %(isilon_user)s@%(isilon_host)s isi statistics drive --nodes=all --long --timestamp --noconversion --csv -i30"
          }
      }

In the above example, notice that host_names_key is not specified since it is being executed only once.
Additionally, there are two commands to collect different types of statistics concurrently.
Finally, since the start command also outputs the result, a stop command is not specified.

The text output of the metrics commands will be stored in output JSON file under the "metrics" key and then
under the agent ID. 

Note that the P3 Test Driver does not parse the metrics output in any way.
All parsing of the metrics is performed by the Test Results Analyzer.

When adding new commands to collect metrics, there are a couple important points. First, ensure that only UTC times are
used so that a time zone conversion does not become necessary. The P3 Test Driver will capture all lines from stdout and
stderr. Additionally, each captured line will have an associated timestamp (in UTC) in case the metrics command
does not write its own timestamp. Lastly, choose the agent ID with consideration to how the data will be parsed
and aggregated.

Refer to the example configuration files in config/example-*.config.json.


*************************
Monitoring Test Execution
*************************

Monitoring the execution of the P3 Test Driver should begin by opening the status HTML file in
a browser. The file name is defined by the status_html configuration parameter. This file is
updated by the P3 Test Driver every few seconds and it will be automatically refreshd by
the browser every few seconds. Simplying opening the status HTML file will result in a
near real-time view of the status of the test batch. It will show the number of completed tests,
the number of warnings and errors, the elapsed time, and other test-specific
information.

When more details are needed for troubleshooting, refer to the P3 Test Driver log file.
The file name is defined by the test_driver_log_filename configuration parameter.

For higher-level monitoring of completed tests, the Kibana interface that is part of the
Test Results Analyzer provides a monitoring dashboard.

Since metrics are parsed only after a test completes, the metrics collected by the P3 Test Driver can't be viewed
in real-time. If this is needed for troubleshooting, it is recommended to use the Linux nmon command
(without parameters) or the isi statistics command directy.


*******************
Plugin Architecture
*******************

New tests and storage systems can be added to the P3 Test Driver using a simple plugin architecture.
See the various Python scripts in the plugins directory for examples, in particular tests/p3_test_simple.py.
For extending the P3 Test Driver to run simple command lines, the Simple Test plugin can be used.

******************
Simple Test Plugin
******************

For simple benchmarks that consist of a single command line to execute, the Simple Test plugin can be used.
The command line can be as complex as the Linux shell allows so multiple commands can be separated with a semicolon,
"&&", "||", etc.. All output will be captured by the P3 Test Driver and it can be parsed by the
Test Results Analyzer. The only requirement for the command is that it should return with an non-zero error
if an error occurs.

For example, the HBase YCSB test is executed using the following parameter:

.. parsed-literal::

  "command_template":
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
    "-jvm-args=-Xmx%(ycsb_heap_MB)dm"

The following parameters are used by the Simple Test plugin.

+-----------------------------+---------------------------------------------------------------------------------------------------------+
| Parameter Name              | Description                                                                                             |
+=============================+=========================================================================================================+
| command                     | The command line to execute. No variable substitution will occur.                                       |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| command_template            | The command line to execute. Variable substitution will occur.                                          |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| command_env                 | Dictionary of environment variables to set when running the command.                                    |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| command_timeout_sec         | If specified, the command will timeout after this many seconds.                                         |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| record_as_test              | The "test" parameter will be set to this value when the results are recorded.                           |
+-----------------------------+---------------------------------------------------------------------------------------------------------+
| test                        | Should be "simple" or "simple_hadoop".                                                                  |
+-----------------------------+---------------------------------------------------------------------------------------------------------+

Refer to tests/testgen_hbase_das.py for a complete example.

*****
HBase
*****

YCSB must be installed for HBase benchmarking. Use the steps below to install it.

.. parsed-literal::

  [root@hop-claudio-cdh1-master-0 p3]# 
  wget https://github.com/brianfrankcooper/YCSB/releases/download/0.4.0/ycsb-0.4.0.tar.gz
  tar -xzvf ~/Downloads/ycsb-0.4.0.tar.gz
  mv ycsb-0.4.0 ycsb
  mkdir ycsb/hbase10-binding/conf
  ln -s /etc/hbase/conf/hbase-site.xml ycsb/hbase10-binding/conf/

To run the HBase tests, use tests/testgen_hbase_das.py.

*********************************
Additional Disk Metrics with nmon
*********************************

nmon 15h has an additional parameter that will gather more detailed disk metrics (-g auto).
Unfortunately, this version has a bug that causes a segfault
(See https://sourceforge.net/p/nmon/discussion/985541/thread/0ea2cb13/).
A patched version is in the nmon folder of the p3 repository.
After compiling it, modify the nmon command to include the "-g auto" parameter.
The test results analyzer will automatically detect and load the additional disk metrics.


************
Optimization
************

This section is not yet documented. 
Refer to tests/testgen\_*optimizer.py for examples of how to use the optimizer.


***********
Isilon Tips
***********

To test different data access patterns or protection levels, the following commands should be run on the Isilon cluster.

.. parsed-literal::

  cd /ifs/isiloncluster1/system/hadoop
  mkdir -p benchmarks/streaming-2d_1n
  mkdir -p benchmarks/concurrency-2d_1n
  isi set -R -p +2d:1n -a streaming   -l streaming   benchmarks/streaming-2d_1n
  isi set -R -p +2d:1n -a default     -l concurrency benchmarks/concurrency-2d_1n
  chmod -R 777 benchmarks

To reduce the number of Isilon nodes in a cluster:

#. The SmartFail process will complete faster if there is no data on the cluster. 
   Delete the benchmark data with the following command:
   hadoop fs -rm -r -skipTrash "/benchmarks/*/*"

#. SmartFail the node(s). 
   To ensure that quorum is maintained, do not SmartFail 50% or more of the nodes at once.

#. Wait for SmartFail to complete and the removed nodes to no longer show up in "isi status".

#. Ensure that the IP address pool has an even number of IP addresses assigned to each NIC and node.
   Using the static IP allocation method will achieve this.

#. Reboot the entire Isilon cluster. This will ensure that old IP addresses are not cached by isi_hdfs_d and that 
   "isi statistics" does not attempt to contact the removed node.
   Sometimes, simply restarting isi_hdfs_d will be enough.

#. Wait for any Isilon jobs to complete.

#. Edit isilon_num_nodes in testgen*.py scripts.

#. After the first benchmark, confirm that the network and disk traffic is equal among all Isilon nodes.
