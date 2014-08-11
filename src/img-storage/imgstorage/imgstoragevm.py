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
from rabbitmqclient import RabbitMQCommonClient, RabbitMQLocator
from imgstorage import *
import logging

import time
import json
import random
import re
import signal
import sys
import traceback
import rocks.db.helper

class VmDaemon():

    def __init__(self):
        self.NODE_NAME = RabbitMQLocator.NODE_NAME
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/null'
        self.stderr_path = '/tmp/err.log'
        self.pidfile_path =  '/var/run/img-storage-vm.pid'
        self.pidfile_timeout = 5
        self.function_dict = {'map_zvol':self.map_zvol, 'unmap_zvol':self.unmap_zvol, 'list_dev':self.list_dev, 'sync_zvol':self.sync_zvol }
        self.logger = logging.getLogger('imgstorage.imgstoragevm.VmDaemon')
        self.sync_enabled = self.is_sync_enabled()

    def is_sync_enabled(self):
        db = rocks.db.helper.DatabaseHelper()
        db.connect()
        sync_enabled = False if db.getHostAttr(db.getHostname(), 'img_sync') is None else db.getHostAttr(db.getHostname(), 'img_sync').lower() == 'true'
        db.close()
        return sync_enabled


    """
    Received map_zvol command from nas
    """
    def map_zvol(self, message, props):
        self.logger.debug("Setting zvol %s"%message['target'])
        try:
            self.connect_iscsi(message['target'], message['nas'])
            mappings = self.get_blk_dev_list()

            if(message['target'] not in mappings.keys()): raise ActionError('Not found %s in targets'%message['target'])

            bdev = '/dev/%s'%mappings[message['target']]
            zvol = message.get('zvol')
            if(self.sync_enabled):
                runCommand(['zfs', 'create', '-V', '%sgb'%message['size'], 'tank/%s'%zvol])
                runCommand(['zfs', 'create', '-V', '10gb', 'tank/%s-temp-write'%zvol])
                time.sleep(5)
                runCommand(['dmsetup', 'create', '%s-snap'%zvol,
                    '--table', '0 62914560 snapshot %s /dev/zvol/tank/%s-temp-write P 16'%(bdev, zvol)])
                bdev = '/dev/mapper/%s-snap'%zvol

            self.queue_connector.publish_message({'action': 'zvol_mapped', 'target':message['target'], 'bdev':bdev, 'status':'success'},
                props.reply_to, reply_to=self.NODE_NAME, correlation_id=props.message_id)

            self.logger.debug('Successfully mapped %s to %s'%(message['target'], bdev))
        except ActionError, msg:
            self.queue_connector.publish_message({'action': 'zvol_mapped', 'target':message['target'], 'status':'error', 'error':str(msg)}, props.reply_to, correlation_id=props.message_id)
            self.logger.error('Error mapping %s: %s'%(message['target'], str(msg)))


    def list_dev(self, message, props):
        mappings_map = self.get_blk_dev_list()
        self.logger.debug("Got mappings %s"%mappings_map)
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
        connect_out = runCommand(['iscsiadm', '-m', 'discovery', '-t', 'sendtargets', '-p', node_name])
        self.logger.debug("Looking for target in iscsiadm output")
        for line in connect_out:
            if iscsi_target in line: #has the target
                self.logger.debug("Found iscsi target in iscsiadm output")
                return runCommand(['iscsiadm', '-m', 'node', '-T', iscsi_target, '-p', node_name, '-l'])
        raise ActionError('Could not find iSCSI target %s on compute node %s'%(iscsi_target, node_name))

    def disconnect_iscsi(self, iscsi_target):
        return runCommand(['iscsiadm', '-m', 'node', '-T', iscsi_target, '-u'])

    """
    Received zvol unmap_zvol command from nas
    """
    def unmap_zvol(self, message, props):
        self.logger.debug("Tearing down zvol %s"%message['target'])

        mappings_map = self.get_blk_dev_list()

        try:
            #if(self.sync_enabled):
                #runCommand(['dmsetup', 'remove', ''])
                #pass
            #else:
                print message['target'] not in mappings_map.keys()
                print self.disconnect_iscsi(message['target'])
                if((message['target'] not in mappings_map.keys()) or self.disconnect_iscsi(message['target'])):
                    self.queue_connector.publish_message({'action': 'zvol_unmapped', 'target':message['target'], 'status':'success'}, props.reply_to, correlation_id=props.message_id)
        except ActionError, msg:
            self.queue_connector.publish_message({'action': 'zvol_unmapped', 'target':message['target'], 'status':'error', 'error':str(msg)}, props.reply_to, correlation_id=props.message_id)
            self.logger.error('Error unmapping %s: %s'%(message['target'], str(msg)))


    def sync_zvol(self, message, props):
        zvol = message.get('zvol')
        target = message.get('target')

        mappings = self.get_blk_dev_list()
        if(target not in mappings.keys()): raise ActionError('Not found %s in targets'%target)

        self.logger.debug("Syncing zvol %s"%zvol)

        try:
            devsize = runCommand(['blockdev', '--getsize', '/dev/%s'%mappings[target]])[0]
            runCommand(['dmsetup', 'suspend', '/dev/mapper/%s-snap'%zvol])
            runCommand(['dmsetup', 'reload', '/dev/mapper/%s-snap'%zvol, '--table', '0 %s snapshot-merge /dev/zvol/tank/%s /dev/zvol/tank/%s-temp-write P 16'%(devsize, zvol, zvol)])
            runCommand(['dmsetup', 'resume', '/dev/mapper/%s-snap'%zvol])
            self.logger.debug('Synced local storage')
            runCommand(['dmsetup', 'suspend', '/dev/mapper/%s-snap'%zvol])
            runCommand(['dmsetup', 'reload', '/dev/mapper/%s-snap'%zvol, '--table', '0 %s linear /dev/zvol/tank/%s 0'%(devsize, zvol)])
            runCommand(['dmsetup', 'resume', '/dev/mapper/%s-snap'%zvol])
            runCommand(['zfs', 'destroy', 'tank/%s-temp-write'%zvol])
            self.disconnect_iscsi(message['target'])

            self.queue_connector.publish_message({'action': 'zvol_synced', 'zvol':zvol, 'status':'success'}, props.reply_to, correlation_id=props.message_id)
        except ActionError, msg:
            self.queue_connector.publish_message({'action': 'zvol_synced', 'zvol':zvol, 'status':'error', 'error':str(msg)}, props.reply_to, correlation_id=props.message_id)
            self.logger.error('Error syncing %s: %s'%(zvol, str(msg)))

    def process_message(self, props, message):
        self.logger.debug("Received message %s"%message)
        if message['action'] not in self.function_dict.keys():
            self.queue_connector.publish_message({'status': 'error', 'error':'action_unsupported'}, exchange='', routing_key=props.reply_to)
            return

        try:
            self.function_dict[message['action']](message, props)
        except:
            self.logger.exception("Unexpected error: %s %s"%(sys.exc_info()[0], sys.exc_info()[1]))
            traceback.print_tb(sys.exc_info()[2])
            self.queue_connector.publish_message({'status': 'error', 'error':sys.exc_info()[1].message}, exchange='', routing_key=props.reply_to, correlation_id=props.message_id)

    def run(self):
        self.queue_connector = RabbitMQCommonClient('rocks.vm-manage', 'direct', self.process_message)
        self.queue_connector.run()

    def stop(self):
        self.queue_connector.stop()
        self.logger.info('RabbitMQ connector stopping called')
