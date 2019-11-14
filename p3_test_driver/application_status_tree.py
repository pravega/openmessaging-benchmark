# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division
import datetime
import logging
import os
import threading
from contextlib import contextmanager
import xml.etree.ElementTree as ElementTree

# P3 Libraries
from system_command import time_duration_to_seconds

def seconds_to_human_string(s):
    return '%0.1f min' % (s/60.0)

def seconds_ago(dt):
    t1 = datetime.datetime.utcnow()
    td = t1 - dt
    return time_duration_to_seconds(td)

class StatusNode(object):
    def __init__(self, parent=None):
        self.parent = parent
        self.status_text = ''
        self.html = None
        self.update_time = datetime.datetime.utcnow()
        self.children = {}

    def set_status(self, status_text, html=None, is_exception=False, is_result=False, destroy_children=False):
        logging.debug('StatusNode.set_status: %s' % status_text)
        self.status_text = status_text
        self.html = html
        self.update_time = datetime.datetime.utcnow()
        if destroy_children:
            self.children = {}

    def create_child(self, child_key=''):
        self.children[child_key] = StatusNode(parent=self)
        return self.children[child_key]

    def flatten(self, level=0):
        result = []
        result.append(dict(level=level, status_text=self.status_text, update_time=self.update_time, html=self.html))
        for child_key, child_node in self.children.items():
            result.extend(child_node.flatten(level+1))
        return result

    def root(self):
        if self.parent:
            return self.parent.root()
        else:
            return self

class StatusTree(StatusNode):
    def __init__(self):
        super(StatusTree, self).__init__()

    def write_status_file(self, status_file):
        flat_tree = self.flatten()
        # logging.debug(str(flat_tree))
        root = ElementTree.Element('html')
        root.append(ElementTree.XML("""
            <head>
                <meta http-equiv="refresh" content="5" />
                <link rel="stylesheet" href="http://yui.yahooapis.com/pure/0.6.0/pure-min.css" />
                <style type="text/css">
                    div.level0 {
                    }
                    div.level1 {
                        margin-left: 2em;
                    }
                    div.level2 {
                        margin-left: 4em;
                    }
                    div.level3 {
                        margin-left: 6em;
                    }
                    div.level4 {
                        margin-left: 8em;
                    }
                    .dataframe tr {
                      line-height: 10%;
                    }
                    .home-menu {
                        padding: 0.5em;
                        text-align: left;
                        box-shadow: 0 1px 1px rgba(0,0,0, 0.10);
                        background: #2d3e50;
                    }
                    .home-menu .pure-menu-heading {
                        color: white;
                        font-weight: 400;
                        font-size: 120%;
                    }
                </style>
            </head>
            """))
        body = ElementTree.Element('body')
        body.append(ElementTree.XML("""
            <div class="header">
                <div class="home-menu pure-menu pure-menu-horizontal">
                    <a class="pure-menu-heading" href="">P3 Test Driver</a>
                </div>
            </div>
            """))
        for node in flat_tree:
            if node['status_text']:
                div = ElementTree.SubElement(body, 'div', attrib={'class': 'level%d' % node['level']})
                div.text = '%s (%s ago)' % (node['status_text'], seconds_to_human_string(seconds_ago(node['update_time'])))
            if node['html']:
                div_html = ElementTree.SubElement(body, 'div', attrib={'class': 'level%d' % node['level']})
                div_html.append(ElementTree.XML(node['html']))
        root.append(body)
        tree = ElementTree.ElementTree(root)
        #logging.debug('write_status_file: writing to file %s' % status_file)
        temp_file_name = '%s.tmp' % status_file
        tree.write(temp_file_name)
        os.rename(temp_file_name, status_file)

class StatusTreeServer(StatusTree):
    def __init__(self, status_file):
        super(StatusTreeServer, self).__init__()
        self.status_file = status_file
        self.stop_event = threading.Event()

    @contextmanager
    def context(self):
        """Context manager to allow 'with' statement to start and stop server thread."""
        self.start()
        yield
        self.stop()

    def start(self):
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True     # thread dies with the program
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join()
        status_file = self.status_file
        if status_file:
            self.status_text = '[COMPLETED] %s' % self.status_text
            self.write_status_file(status_file)

    def _run(self):
        while True:
            status_file = self.status_file
            if status_file:
                self.write_status_file(status_file)
            self.stop_event.wait(5.0)
            if self.stop_event.is_set():
                return
