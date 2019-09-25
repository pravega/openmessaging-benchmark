# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import logging
import subprocess
import queue as Queue
import threading

# P3 Libraries
from system_command import popen_to_queue, system_command, ssh, BackgroundProcess

class MetricsAgent:
    """Represents a single process that is executed to produce metrics on stdout or stderr"""

    def __init__(self, queue, agent_id, cmd, stop_cmd=None):
        self.queue = queue
        self.agent_id = agent_id
        self.cmd = cmd
        self.stop_cmd = stop_cmd
        self.stop_process = None

    def start(self):
        logging.info('MetricsAgent.start: %s: %s' % (self.agent_id, self.cmd))
        # Start process
        self.start_process = BackgroundProcess(self.queue, self.cmd, shell=True, message_data={'agent_id': self.agent_id, 'process': 'start'})

    def launch_stop_process(self):
        logging.info('MetricsAgent.launch_stop_process: %s' % self.agent_id)
        if self.stop_cmd:
            # Launch stop process
            logging.info('MetricsAgent.launch_stop_process: %s: %s' % (self.agent_id, self.stop_cmd))
            self.stop_process = BackgroundProcess(self.queue, self.stop_cmd, shell=True, message_data={'agent_id': self.agent_id, 'process': 'stop'})

    def stop(self):
        logging.info('MetricsAgent.stop: %s' % self.agent_id)
        if self.stop_process:
            # Wait for stop process
            logging.info('MetricsAgent.stop: %s: %s' % (self.agent_id, self.stop_cmd))
            self.stop_process.wait()
        self.start_process.stop()

class MetricsCollector:
    """Represents a collection of MetricsAgents whose output should all be collected into a single dictionary object."""
    
    def __init__(self, results):
        self.results = results
        self.agents = {}

    def start(self):
        logging.info('MetricsCollector.start')
        self.queue = Queue.Queue()
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True     # thread dies with the program
        self.thread.start()

    def run(self):
        while True:
            item = self.queue.get()
            logging.log(0, 'MetricsCollector.run: Received message: %s' % item)
            message_type = item['message_type']
            if message_type == 'stop':
                break
            agent_id = item['agent_id']
            results_format = self.agents[agent_id]['results_format']
            if results_format == 'text':
                # We build the output with a list of lines and then convert to a single string in finalize_results().
                if message_type == 'system_command_output':
                    self.results[agent_id]['output'].append(item['line'])
                    self.results[agent_id]['output_utc'].append(item['utc'])                    
                elif message_type == 'system_command_error':
                    self.results[agent_id]['errors'].append(item['line'])
                    self.results[agent_id]['errors_utc'].append(item['utc'])                    
                elif message_type == 'system_command_result' and item['process'] == 'start' and not item['returncode'] is None:
                    self.results[agent_id]['returncode'] = item['returncode']
            else:
                self.results[agent_id].append(item)

    def add_agent(self, agent_id, cmd, stop_cmd, results_format='text'):
        agent = MetricsAgent(self.queue, agent_id, cmd, stop_cmd)
        self.agents[agent_id] = {'agent': agent, 'results_format': results_format}
        if results_format == 'text':
            self.results[agent_id] = {'output': [], 'output_utc': [], 'errors': [], 'errors_utc': [], 'returncode': None}
        else:
            self.results[agent_id] = []
        agent.start()

    def stop(self):
        logging.info('MetricsCollector.stop: Stopping agents and collector...')
        for agent_info in self.agents.values():
            agent_info['agent'].launch_stop_process()            
        for agent_info in self.agents.values():
            agent_info['agent'].stop()            
        self.queue.put({'message_type': 'stop'})
        self.thread.join()
        self.finalize_results()
        self.print_status()
        logging.info('MetricsCollector.stop: Shutdown complete.')

    def finalize_results(self):
        for agent_id, agent_info in self.agents.items():
            results_format = agent_info['results_format']
            if results_format == 'text':
                # Convert list of lines to single string with new lines.
                for field in ['output','errors','output_utc','errors_utc']:
                    self.results[agent_id][field].append('')
                    self.results[agent_id][field] = '\n'.join(self.results[agent_id][field])

    def print_status(self):
        for agent_id, agent_info in self.agents.items():
            results_format = agent_info['results_format']
            if results_format == 'text':
                logging.info('MetricsCollector: %s: %d bytes collected' % 
                    (agent_id, (len(self.results[agent_id]['output']) + len(self.results[agent_id]['errors']))))
            else:
                logging.info('MetricsCollector: %s: %d records collected' % len(agent_id, self.results[agent_id]))
