# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import json
import logging
import requests
import xmltodict
import tempfile
import os
from sklearn.externals.joblib import Parallel, delayed

# P3 Libraries
from system_command import system_command, ssh
from p3_util import get_remote_text_file_contents, mkdir_p, configure_sysctl, configure_sys_files, unflatten_dict_keys

def kill_all_yarn_jobs():
    system_command('yarn application -list | grep application_ | awk \' { system("yarn application -kill " $1) } \'',
        print_output=True)

def kill_yarn_job(application_id):
    system_command('yarn application -kill %s' % application_id, print_output=True)

def configure_compute(config):
    # TODO: Need to bring in code for resizing cluster from hadoop-test-driver.pl

    collect_compute_node_info(config)

    num_compute_nodes = get_num_compute_nodes(config)
    logging.info('configure_compute: actual num_compute_nodes=%d' % num_compute_nodes)
    if config.get('num_compute_nodes') and config.get('num_compute_nodes') != num_compute_nodes:
        raise Exception('Actual num_compute_nodes (%d) does not match requested num_compute_nodes (%d)' % (num_compute_nodes, config.get('num_compute_nodes')))
    config['num_compute_nodes'] = num_compute_nodes

    if config.get('kill_all_yarn_jobs',False):
        kill_all_yarn_jobs()

    configure_node_managers(config)
    flush_compute_cache(config)

def collect_remote_text_file_helper(host_name, filename, user='root'):
    contents = get_remote_text_file_contents(filename, user, host_name)
    return (host_name, filename, contents)

def collect_compute_node_info(config):
    config['hadoop_version'] = system_command('hadoop version | head -1', print_output=True)[1].replace('\n','')
    config['hive_version'] = system_command('hive --version | head -1', print_output=True)[1].replace('\n','')
    config['impala_version'] = system_command('impala-shell --version | head -1', print_output=True, raise_on_error=False)[1].replace('\n','')

    hadoop_client_host = config.get('hadoop_client_host', 'localhost')
    hadoop_ws_port = config.get('hadoop_ws_port', 8088)
    base_yarn_url = 'http://%s:%d' % (hadoop_client_host, hadoop_ws_port)

    url = '%s/conf' % base_yarn_url
    print('collect_compute_node_info: Connecting to %s' % url)
    conf_xml = requests.get(url).content
    conf = xmltodict.parse(conf_xml)
    config['hadoop_resource_manager_conf'] = conf

    url = '%s/ws/v1/cluster/nodes' % base_yarn_url
    print('collect_compute_node_info: Connecting to %s' % url)
    info = json.loads(requests.get(url).content)
    config['compute_node_info'] = info

    # Collect file contents from each node manager in parallel.
    node_manager_host_names = get_compute_node_host_names(config)
    filenames = config.get('collect_text_files_node_manager',[])
    pjobs = [delayed(collect_remote_text_file_helper)(host_name, filename)
        for filename in filenames
        for host_name in node_manager_host_names]        
    logging.info('Collecting contents of %d files' % len(pjobs))
    file_contents = Parallel(n_jobs=100)(pjobs) if pjobs else []
    # Store file contents in config.
    config['collected_text_files_node_manager'] = {filename: {} for filename in filenames}
    for host_name, filename, contents in file_contents:
        if not contents is None:
            logging.info('collect_compute_node_info: %s:%s %d bytes collected' % 
                (host_name, filename, len(contents)))
        config['collected_text_files_node_manager'][filename][host_name] = contents

def get_compute_node_info(config, force=False):
    if not 'compute_node_info' in config or force:
        collect_compute_node_info(config)
    return config['compute_node_info']

# Get active compute nodes (node managers, workers).
def get_compute_node_host_names(config):
    info = get_compute_node_info(config)
    host_names = []
    for node in info['nodes']['node']:
        if node['state'] == 'RUNNING':
            host_names.append(node['nodeHostName'])
    config['compute_node_host_names'] = sorted(host_names)
    return config['compute_node_host_names']

def get_num_compute_nodes(config):
    return len(get_compute_node_host_names(config))

def try_request_get_json(*args, **kwargs):
    try:
        return json.loads(requests.get(*args, **kwargs).content)
    except:
        return None

def get_completed_job_info(config):
    if 'hadoop_job_id' in config:
        mapred_history_host = config.get('mapred_history_host', 'localhost')
        mapred_history_port = config.get('mapred_history_port', 19888)
        base_history_url = 'http://%s:%d' % (mapred_history_host, mapred_history_port)
        job_history_url = '%s/ws/v1/history/mapreduce/jobs/%s' % (base_history_url, config['hadoop_job_id'])
        logging.info('Collecting job info from %s' % job_history_url)
        config['hadoop_job_info'] = try_request_get_json(job_history_url)
        config['hadoop_job_conf'] = try_request_get_json('%s/conf' % job_history_url)
        config['hadoop_job_counters'] = try_request_get_json('%s/counters' % job_history_url)
        config['hadoop_job_tasks'] = try_request_get_json('%s/tasks' % job_history_url)
        if config.get('mapred_log_collect',False):
            collect_mapred_logs(config)

def collect_mapred_logs(config):
    mapred_log_dir = config['mapred_log_dir']
    hadoop_job_id = config['hadoop_job_id']
    test_uuid = config['test_uuid']
    temp_dir = tempfile.mkdtemp()
    subdir = '%s--%s' % (hadoop_job_id, test_uuid)
    temp_log_dir = os.path.join(temp_dir, subdir)
    os.mkdir(temp_log_dir)
    mkdir_p(mapred_log_dir)
    mapred_log_archive_file = os.path.join(mapred_log_dir, '%s--%s.tar.bz2' % (hadoop_job_id, test_uuid))
    logging.info('collect_mapred_logs: Downloading logs to %s' % temp_log_dir)
    system_command('mapred job -list-attempt-ids %s MAP    completed | xargs -P 50 -n 1 -I{} sh -c "mapred job -logs %s {} > %s/{}.log"'
        % (hadoop_job_id, hadoop_job_id, temp_log_dir), print_output=True)
    system_command('mapred job -list-attempt-ids %s REDUCE completed | xargs -P 50 -n 1 -I{} sh -c "mapred job -logs %s {} > %s/{}.log"'
        % (hadoop_job_id, hadoop_job_id, temp_log_dir), print_output=True)
    system_command('mapred job -logs %s > %s/appmaster.log' % (hadoop_job_id, temp_log_dir), print_output=True)
    logging.info('collect_mapred_logs: Archiving logs to %s' % mapred_log_archive_file)
    system_command('tar -cjvf %s -C %s %s' % (mapred_log_archive_file, temp_dir, subdir), print_output=True)
    config['mapred_log_archive_file'] = mapred_log_archive_file

def flush_linux_caches(host_name, user='root'):
    cmd = 'sync ; sysctl -w vm.drop_caches=3'
    exit_code, output = ssh(user, host_name, cmd)
    if exit_code:
        raise Exception('Unable to drop caches on %s' % host_name)

def flush_compute_cache(config):
    """Flush linux caches in parallel."""
    if config.get('flush_compute',False):
        host_names = get_compute_node_host_names(config)
        logging.info('Flushing caches on %d hosts' % len(host_names))
        pjobs = [delayed(flush_linux_caches)(host_name) for host_name in host_names]
        Parallel(n_jobs=len(pjobs))(pjobs)

def configure_node_manager(host_name, sysctl_settings, sys_settings, transparent_hugepage_enabled, user='root'):
    configure_sysctl(user, host_name, sysctl_settings)
    configure_sys_files(user, host_name, sys_settings)
    if not transparent_hugepage_enabled is None:            
        if transparent_hugepage_enabled:
            val = 'always'
        else:
            val = 'never'
        cmd = (
            'if test -f /sys/kernel/mm/redhat_transparent_hugepage/defrag; then echo %(val)s > /sys/kernel/mm/redhat_transparent_hugepage/defrag; fi; ' +
            'if test -f /sys/kernel/mm/redhat_transparent_hugepage/enabled; then echo %(val)s > /sys/kernel/mm/redhat_transparent_hugepage/enabled; fi; ' +
            'if test -f /sys/kernel/mm/transparent_hugepage/defrag; then echo %(val)s > /sys/kernel/mm/transparent_hugepage/defrag; fi; ' +
            'if test -f /sys/kernel/mm/transparent_hugepage/enabled; then echo %(val)s > /sys/kernel/mm/transparent_hugepage/enabled; fi; ' +
            'cat /sys/kernel/mm/transparent_hugepage/enabled'
            ) % {'val': val}
        ssh(user, host_name, cmd, print_output=True, raise_on_error=True)

def configure_node_managers(config):
    """Configure node managers in parallel."""
    host_names = get_compute_node_host_names(config)
    sysctl_settings = unflatten_dict_keys(config, 'sysctl_(.*)')
    sys_settings = unflatten_dict_keys(config, '(/sys/.*)')
    pjobs = [delayed(configure_node_manager)(host_name, sysctl_settings, sys_settings, config.get('transparent_hugepage_enabled')) for host_name in host_names]
    Parallel(n_jobs=len(pjobs))(pjobs)
