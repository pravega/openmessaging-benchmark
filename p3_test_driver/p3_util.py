# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import uuid
import datetime
import re
import logging
import six
import os
import errno
import itertools
import glob

# P3 Libraries
from json_util import append_to_json_file
from system_command import ssh, system_command

def regex_groups(regex, s, return_on_no_match=None, flags=0, search=False, match_last=False):
    if isinstance(s, six.string_types):
        if search and match_last:
            matches = list(re.finditer(regex, s, flags=flags))
            if matches:
                m = matches[-1]
                return m.groups()
            else:
                return return_on_no_match
        if search:
            m = re.search(regex, s, flags=flags)
        else:
            m = re.match(regex, s, flags=flags)
        if m:
            return m.groups()
    return return_on_no_match

def regex_first_group(regex, s, return_on_no_match=None, flags=0, search=False, match_last=False):
    g = regex_groups(regex, s, return_on_no_match=[return_on_no_match], flags=flags, search=search, match_last=match_last)
    return g[0]

def public_dict(d):
    """Returns a copy of a dict without any private keys. Private keys begin with an underline."""
    return dict((k,v) for k,v in d.items() if not k.startswith('_'))

def record_result(result, result_filename):
    if 'record_uuid' not in result:
        result['record_uuid'] = str(uuid.uuid4())
    now = datetime.datetime.utcnow()
    if 'record_utc' not in result:
        result['record_utc'] = now.isoformat()
    rec = public_dict(result)
    filename_timestamp = now.strftime('%Y%m%d%H%M%S%f')
    var_dict = result.copy()
    var_dict['timestamp'] = filename_timestamp
    result_filename = result_filename % var_dict
    logging.info('Recording results to file %s' % result_filename)
    mkdir_for_file(result_filename)
    append_to_json_file([rec], result_filename, sort_keys=True, indent=True)

    post_command = result.get('record_result_post_command')
    if post_command:
        var_dict['result_filename'] = result_filename
        post_command = post_command % var_dict
        system_command(post_command, print_command=True, print_output=True, raise_on_error=False, shell=True)

def get_remote_text_file_contents(filename, user=None, host=None):
    cmd = 'cat %s' % filename
    returncode, output, errors = ssh(user, host, cmd, print_output=False, raise_on_error=False, stderr_to_stdout=False)
    if returncode == 0:
        return output
    else:
        return None

def mkdir_p(path):
    """From http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python"""
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def mkdir_for_file(filename):
    """Create directories to contain the specified file."""
    directory = os.path.dirname(filename)
    if directory:
        mkdir_p(directory)

def updated_dict(d, updates):
    updated = d.copy()
    updated.update(updates)
    return updated
    
def read_file_to_string(filename):
    with open(filename, 'r') as f:
        return f.read()

def grid_to_config_list(grid):
    values = [v for x in grid for f,v in x.items()]
    field_names = [f for x in grid for f,v in x.items()]
    all_values = itertools.product(*values)
    config_list = [zip(field_names, x) for x in all_values]
    config_list = [dict([f for f in x if f[0]]) for x in config_list]
    return config_list

def configure_sysctl(user, host, settings):
    if settings:
        args = ['sysctl -w %s="%s"' % (k,v) for k,v in settings.iteritems()]
        cmd = ' && '.join(args)
        ssh(user, host, cmd)

def configure_sys_files(user, host, settings):
    """This function can echo values to files. For instance: 
    settings = {"/sys/block/sd*/queue/nr_requests": 128}
    """
    if settings:
        args = ['for f in %s ; do (echo %s > \\$f) ; done' % (k,v) for k,v in settings.iteritems()]
        cmd = ' && '.join(args)
        ssh(user, host, cmd)

def unflatten_dict_keys(d, key_regex):
    """Extract key/value pairs from dict d whose key match key_regex. Keys will be renamed to first group returned by regex."""
    result = {}
    for k,v in d.iteritems():
        new_k = regex_first_group(key_regex, k)
        if new_k:
            result[new_k] = v
    return result

def glob_file_list(filespecs):
    if not isinstance(filespecs,list):
        filespecs = [filespecs]
    return sum(map(glob.glob, filespecs), [])
