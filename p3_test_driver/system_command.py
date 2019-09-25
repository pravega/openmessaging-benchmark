# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import subprocess
import sys
import datetime
import Queue
import logging
import threading

def time_duration_to_seconds(td):
    return (td.microseconds + (td.seconds + td.days * 24 * 60 * 60) * 10.**6) / 10.**6

def popen_to_queue(popen, pipe, q, message_type='system_command_output', message_data=None, send_return_code=True):
    """Warning: If watching stdout and stderr, only one should have send_return_code=True. Otherwise, p.returncode will be inconsistent."""
    if message_data is None:
        message_data = {}
    for line in iter(pipe.readline, b''):
        line = line.rstrip('\n')
        msg = message_data.copy()
        msg.update({'message_type': message_type, 'line': line, 'utc': datetime.datetime.utcnow().isoformat()})
        # print('send: %s' % msg)
        q.put(msg)
    return_code = None
    if send_return_code:
        try:
            popen.wait()
        except:
            pass
        return_code = popen.returncode
    msg = message_data.copy()
    msg.update({'message_type': 'system_command_result', 'returncode': return_code, 'utc': datetime.datetime.utcnow().isoformat()})
    q.put(msg)

def system_command(cmd, print_command=True, print_output=False, raise_on_error=True, shell=True, timeout=None, noop=False, env=None):
    """Execute an external application with options to print the output as it comes or kill
    the application after a timeout.
    Returns the tuple (return_code, output, errors). return_code is -1 on timeout."""
    
    if print_command:
        if isinstance(cmd, list):
            logging.info('# %s' % ' '.join(cmd))
        else:
            logging.info('# %s' % cmd)

    if noop:
        return 1, '', ''

    p = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

    timed_out = False
    
    # Determine if we need to use our custom event handler or p.communicate().
    if timeout or print_output:
        if not timeout or timeout <= 0: timeout = sys.maxint
        t0 = datetime.datetime.utcnow()       
        # Create queue for process output
        q = Queue.Queue()
        # launch threads to place popen output into queue
        thread_stdout = threading.Thread(target=lambda: popen_to_queue(p, p.stdout, q))
        thread_stdout.daemon = True     # thread dies with the program
        thread_stdout.start()
        thread_stderr = threading.Thread(target=lambda: popen_to_queue(p, p.stderr, q, message_type='system_command_error', send_return_code=False))
        thread_stderr.daemon = True     # thread dies with the program
        thread_stderr.start()
        output = ''
        errors = ''
        # Receive from queue
        num_open_streams = 2
        while True:
            t1 = datetime.datetime.utcnow()   
            td = t1 - t0
            td_sec = time_duration_to_seconds(td)
            time_left = timeout - td_sec
            if time_left < 0.0:
                logging.warn('Timeout while waiting for system command to complete')
                timed_out = True
                break
            try:
                item = q.get(timeout=time_left)
                # print('receive: %s' % item)
                message_type = item['message_type']
                if message_type == 'system_command_output':
                    line = item['line']
                    if print_output:
                        logging.info(line)
                    output += line
                    output += '\n'
                elif message_type == 'system_command_error':
                    line = item['line']
                    if print_output:
                        logging.info(line)
                    errors += line
                    errors += '\n'
                elif message_type == 'system_command_result':
                    num_open_streams -= 1
                    if num_open_streams <= 0:
                        break
                    # Note that sometimes, we can a non-zero returncode here but p.returncode is 0.
            except Queue.Empty:
                pass
        # Either the command finished or timed out.
        try:
            p.kill()
        except:
            pass
        thread_stdout.join()
        thread_stderr.join()
    else:
        output, errors = p.communicate()
        try:
            p.kill()
        except:
            pass
    
    if timed_out:
        if raise_on_error:
            raise Exception('System command timed out after %0.0f seconds: %s' % (timeout, cmd))
        return_code = -1
    else:
        return_code = p.returncode
    
    if raise_on_error and return_code != 0:
        raise Exception('System command returned %d: %s' % (return_code, cmd))

    return return_code, output, errors

def ssh(user, host, command, opts='', raise_on_error=True, stderr_to_stdout=True, print_output=True, secure=True):
    escaped_command = command.replace('"', '\\"')
    opts_with_space = ''
    if opts: opts_with_space = '%s ' % opts
    if not secure: opts_with_space += '-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null '
    cmd = 'ssh %s%s@%s "%s"' % (opts_with_space, user, host, escaped_command)
    if stderr_to_stdout:
        cmd += ' 2>&1'
    returncode, output, errors = system_command(cmd, print_command=True, raise_on_error=raise_on_error, print_output=print_output)
    if not raise_on_error:
        logging.info('Exit code=%d' % returncode)
    if stderr_to_stdout:
        return (returncode, output)
    else:
        return (returncode, output, errors)

class BackgroundProcess:
    def __init__(self, queue, cmd, shell=True, message_data=None, start=True):
        self.queue = queue
        self.cmd = cmd
        self.shell = shell
        self.message_data = message_data
        if start:
            self.start()

    def start(self):
        # Start process
        self.popen = subprocess.Popen(self.cmd, shell=self.shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Start thread to monitor stdout
        self.thread_stdout = threading.Thread(target=lambda: popen_to_queue(
            self.popen, self.popen.stdout, self.queue, message_data=self.message_data))
        self.thread_stdout.daemon = True     # thread dies with the program
        self.thread_stdout.start()
        # Start thread to monitor stderr
        self.thread_stderr = threading.Thread(target=lambda: popen_to_queue(
            self.popen, self.popen.stderr, self.queue, message_data=self.message_data, 
            message_type='system_command_error', send_return_code=False))
        self.thread_stderr.daemon = True     # thread dies with the program
        self.thread_stderr.start()

    def wait(self):
        self.thread_stdout.join()
        self.thread_stderr.join()

    def stop(self):
        try:
            self.popen.kill()
        except:
            pass
        self.wait()
