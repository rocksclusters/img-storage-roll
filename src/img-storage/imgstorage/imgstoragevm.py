#!/opt/rocks/bin/python
#
# @Copyright@
#
#                               Rocks(r)
#                        www.rocksclusters.org
#                        version 5.6 (Emerald Boa)
#                        version 6.1 (Emerald Boa)
#
# Copyright (c) 2000 - 2013 The Regents of the University of California.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# 1. Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
# notice unmodified and in its entirety, this list of conditions and the
# following disclaimer in the documentation and/or other materials provided
# with the distribution.
#
# 3. All advertising and press materials, printed or electronic, mentioning
# features or use of this software must display the following acknowledgement:
#
#       "This product includes software developed by the Rocks(r)
#       Cluster Group at the San Diego Supercomputer Center at the
#       University of California, San Diego and its contributors."
#
# 4. Except as permitted for the purposes of acknowledgment in paragraph 3,
# neither the name or logo of this software nor the names of its
# authors may be used to endorse or promote products derived from this
# software without specific prior written permission.  The name of the
# software includes the following terms, and any derivatives thereof:
# "Rocks", "Rocks Clusters", and "Avalanche Installer".  For licensing of
# the associated name, interested parties should contact Technology
# Transfer & Intellectual Property Services, University of California,
# San Diego, 9500 Gilman Drive, Mail Code 0910, La Jolla, CA 92093-0910,
# Ph: (858) 534-5815, FAX: (858) 534-7345, E-MAIL:invent@ucsd.edu
#
# THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS''
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
# IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# @Copyright@
#
from rabbitmqclient import RabbitMQCommonClient
from imgstorage import *
import logging

import time
import json
import random
import re
import signal
import sys
import traceback

class VmDaemon():

    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/null'
        self.stderr_path = '/tmp/err.log'
        self.pidfile_path =  '/var/run/img-storage-vm.pid'
        self.pidfile_timeout = 5
        self.function_dict = {'set_zvol':self.set_zvol, 'tear_down':self.tear_down, 'list_dev':self.list_dev }

    """
    Received set_zvol command from nas
    """
    def set_zvol(self, message, props):
        logger.debug("Setting zvol %s"%message['target'])
        try:
            self.connect_iscsi(message['target'], message['nas'])
            mappings = self.get_blk_dev_list()

            if(message['target'] not in mappings.keys()): raise ActionError('Not found %s in targets'%message['target'])

            self.queue_connector.publish_message({'action': 'zvol_attached', 'target':message['target'], 'bdev':mappings[message['target']], 'status':'success'}, props.reply_to, correlation_id=props.message_id)
            logger.debug('Successfully mapped %s to %s'%(message['target'], mappings[message['target']]))
        except ActionError, msg:
            self.queue_connector.publish_message({'action': 'zvol_attached', 'target':message['target'], 'status':'error', 'error':str(msg)}, props.reply_to, correlation_id=props.message_id)
            logger.error('Error mapping %s: %s'%(message['target'], str(msg)))


    def list_dev(self, message, props):
        mappings_map = self.get_blk_dev_list()
        logger.debug("Got mappings %s"%mappings_map)
        mappings_ar = []
        for target in mappings_map.keys():
            mappings_ar.append({'target':target, 'device':mappings_map[target]})
        self.queue_connector.publish_message({'action': 'dev_list', 'status': 'success', 'body':mappings_ar}, exchange='', routing_key=props.reply_to)

    def get_blk_dev_list(self):
    try:
            out = runCommand(['iscsiadm', '-m', 'session', '-P3'])
        except:
            return {}
        mappings = {}
        cur_target = None
        for line in out:
                if "Target: " in line:
                        cur_target = re.search(r'Target: ([\w\-\.]*)$',line, re.M).group(1)
                if 'Attached scsi disk ' in line:
                        blockdev = re.search( r'Attached scsi disk (\w*)', line, re.M)
                        mappings[cur_target] = blockdev.group(1)
        return mappings

    def connect_iscsi(self, iscsi_target, node_name):
        connect_out = runCommand(['iscsiadm', '--mode', 'discovery', '--type', 'sendtargets', '-p', node_name])
        logger.debug("Looking for target in iscsiadm output")
        for line in connect_out:
            if iscsi_target in line: #has the target
                logger.debug("Found iscsi target in iscsiadm output")
                return runCommand(['iscsiadm', '-m', 'node', '-T', iscsi_target, '-p', node_name, '-l'])
        raise ActionError('Could not find iSCSI target %s on compute node %s'%(iscsi_target, node_name))

    def disconnect_iscsi(self, iscsi_target):
        return runCommand(['iscsiadm', '-m', 'node', '-T', iscsi_target, '-u'])

    """
    Received zvol tear_down command from nas
    """
    def tear_down(self, message, props):
        logger.debug("Tearing down zvol %s"%message['target'])

        mappings_map = self.get_blk_dev_list()

        if((message['target'] not in mappings_map.keys()) or self.disconnect_iscsi(message['target'])):
            self.queue_connector.publish_message({'action': 'zvol_detached', 'target':message['target'], 'status':'success'}, props.reply_to, correlation_id=props.message_id)
        else:
            logger.error("error detaching the target %s"%message['target'])
            self.queue_connector.publish_message({'action': 'zvol_detached', 'target':message['target'], 'status':'error', 'error':'can_not_detach'}, props.reply_to, correlation_id=props.message_id)

    def process_message(self, props, message):
        logger.debug("Received message %s"%message)
        if message['action'] not in self.function_dict.keys():
            self.queue_connector.publish_message({'status': 'error', 'error':'action_unsupported'}, exchange='', routing_key=props.reply_to)
            return

        try:
            self.function_dict[message['action']](message, props)
        except:
            logger.error("Unexpected error: %s %s"%(sys.exc_info()[0], sys.exc_info()[1]))
            traceback.print_tb(sys.exc_info()[2])
            self.queue_connector.publish_message({'status': 'error', 'error':sys.exc_info()[1].message}, exchange='', routing_key=props.reply_to, correlation_id=props.message_id)

    def run(self):
        self.queue_connector = RabbitMQCommonClient('rocks.vm-manage', 'direct', self.process_message)
        self.queue_connector.run()

    def stop(self):
        self.queue_connector.stop()
        logger.info('RabbitMQ connector stopping called')
