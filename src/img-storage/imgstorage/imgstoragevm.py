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

from pysqlite2 import dbapi2 as sqlite3

class VmDaemon():

    def __init__(self):
        self.NODE_NAME = RabbitMQLocator.NODE_NAME
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/null'
        self.stderr_path = '/tmp/err.log'
        self.pidfile_path =  '/var/run/img-storage-vm.pid'
        self.pidfile_timeout = 5
        self.function_dict = {'map_zvol':self.map_zvol, 'unmap_zvol':self.unmap_zvol, 'list_dev':self.list_dev, 'list_vdev':self.list_vdev, 'sync_zvol':self.sync_zvol, 'list_sync':self.list_sync }
        self.logger = logging.getLogger('imgstorage.imgstoragevm.VmDaemon')
        self.sync_enabled = self.is_sync_enabled()
        self.SQLITE_DB = '/opt/rocks/var/img_storage.db'
        self.ZPOOL = RabbitMQLocator.ZPOOL

        self.temp_size = 35

        self.SYNC_CHECK_TIMEOUT = 10

        rocks.db.helper.DatabaseHelper().closeSession() # to reopen after daemonization

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
        zvol = message.get('zvol')
        try:
            self.connect_iscsi(message['target'], message['nas'])
            mappings = self.get_blk_dev_list()

            if(message['target'] not in mappings.keys()): raise ActionError('Not found %s in targets'%message['target'])

            bdev = '/dev/%s'%mappings[message['target']]

            if(self.sync_enabled):
                temp_size_cur = min(self.temp_size, int(message['size'])-1)
                runCommand(['zfs', 'create', '-V', '%sgb'%message['size'], '%s/%s'%(self.ZPOOL, zvol)])
                runCommand(['zfs', 'create', '-V', '%sgb'%temp_size_cur, '%s/%s-temp-write'%(self.ZPOOL, zvol)])
                time.sleep(2)
                runCommand(['dmsetup', 'create', '%s-snap'%zvol,
                    '--table', '0 %s snapshot %s /dev/zvol/%s/%s-temp-write P 16'%(int(1024**3*temp_size_cur/512), bdev, self.ZPOOL, zvol)])
                bdev = '/dev/mapper/%s-snap'%zvol

            self.queue_connector.publish_message({'action': 'zvol_mapped', 'target':message['target'], 'bdev':bdev, 'status':'success'},
                props.reply_to, reply_to=self.NODE_NAME, correlation_id=props.message_id)

            self.logger.debug('Successfully mapped %s to %s'%(message['target'], bdev))
        except ActionError, msg:
            self.queue_connector.publish_message({'action': 'zvol_mapped', 'target':message['target'], 'status':'error', 'error':str(msg)}, props.reply_to, reply_to=self.NODE_NAME, correlation_id=props.message_id)
            self.logger.exception('Error mapping %s: %s'%(message['target'], str(msg)))


    def list_dev(self, message, props):
        mappings_map = self.get_blk_dev_list()
        self.logger.debug("Got mappings %s"%mappings_map)
        mappings_ar = []
        for target in mappings_map.keys():
            mappings_ar.append({'target':target, 'device':mappings_map[target]})
        self.queue_connector.publish_message({'action': 'dev_list', 'status': 'success', 'body':mappings_ar}, exchange='', routing_key=props.reply_to)

    def list_vdev(self, message, props):
        mappings_map = self.get_vdev_list()
        self.logger.debug("Got mappings %s"%mappings_map)
        self.queue_connector.publish_message({'action': 'vdev_list', 'status': 'success', 'body':mappings_map}, exchange='', routing_key=props.reply_to)


    def list_sync(self, message, properties):
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('SELECT zvol, iscsi_target, started, time from sync_queue ORDER BY sync_queue.time ASC;')
            r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
            self.queue_connector.publish_message({'action': 'return_sync', 'status': 'success', 'body':r}, exchange='', routing_key=properties.reply_to)
            self.logger.debug(r)

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

    def get_vdev_list(self):
        try:
            out = runCommand(['dmsetup', 'status'])
        except:
            return {}
        mappings = {}
        if(out[0] == "No devices found"): return {}
        for line in out:
            dev_ar = line.split()
            dev_name = dev_ar[0][:-1]
            mappings[dev_name] = {
               'status': dev_ar[3],
               'size': int(dev_ar[2])*512/(1024**3) 
            }
            if(dev_ar[3] != 'linear'):
                mappings[dev_name]['synced'] = "%s %s"%(dev_ar[4], dev_ar[5])
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
        zvol = message['zvol']
        mappings_map = self.get_blk_dev_list()

        try:
            if(self.sync_enabled):
                try:
                    runCommand(['dmsetup', 'remove', '%s-snap'%zvol])
                except ActionError, msg:
                    self.logger.exception(msg)

                self.queue_connector.publish_message({'action': 'zvol_unmapped', 'target':message['target'], 'zvol':zvol, 'status':'success'}, props.reply_to, reply_to=self.NODE_NAME, correlation_id=props.message_id)
            else:
                if((message['target'] not in mappings_map.keys()) or self.disconnect_iscsi(message['target'])):
                    self.queue_connector.publish_message({'action': 'zvol_unmapped', 'target':message['target'], 'zvol':zvol, 'status':'success'}, props.reply_to, reply_to=self.NODE_NAME, correlation_id=props.message_id)
        except ActionError, msg:
            self.queue_connector.publish_message({'action': 'zvol_unmapped', 'target':message['target'], 'zvol':zvol, 'status':'error', 'error':str(msg)}, props.reply_to, reply_to=self.NODE_NAME, correlation_id=props.message_id)
            self.logger.error('Error unmapping %s: %s'%(message['target'], str(msg)))


    def sync_zvol(self, message, props):
        zvol = message.get('zvol')
        target = message.get('target')

        mappings = self.get_blk_dev_list()
        try:
            if(target not in mappings.keys()):
                raise ActionError('Not found %s in targets'%target)

            devsize = runCommand(['blockdev', '--getsize', '/dev/%s'%mappings[target]])[0]

            with sqlite3.connect(self.SQLITE_DB) as con:
                cur = con.cursor()
                cur.execute('INSERT INTO sync_queue VALUES(?,?,?,?,?,0,?)',
                        [zvol, target, devsize, props.reply_to, props.message_id, time.time()])
                self.logger.debug("Updated the db for zvol %s : %s"%(zvol, devsize))
                con.commit()
        except ActionError, msg:
            self.queue_connector.publish_message({'action': 'zvol_synced', 'zvol':zvol, 'status':'error', 'error':str(msg)}, 
                    props.reply_to, correlation_id=props.message_id)

            self.logger.exception('Error syncing %s: %s'%(zvol, str(msg)))
 

    def run_sync(self):
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('select zvol, iscsi_target, devsize, reply_to, correlation_id, started from sync_queue ORDER BY time ASC LIMIT 1')
            row = cur.fetchone()
            if(row):
                zvol, target, devsize, reply_to, correlation_id, started = row

                try:
                    start = time.time()
                    if(not started):
                        self.logger.debug("Starting new sync %s"%zvol)
                        runCommand(['dmsetup', 'suspend', '/dev/mapper/%s-snap'%zvol])
                        runCommand(['dmsetup', 'reload', '/dev/mapper/%s-snap'%zvol, '--table', '0 %s snapshot-merge /dev/zvol/%s/%s /dev/zvol/%s/%s-temp-write P 16'%(devsize, self.ZPOOL, zvol, self.ZPOOL, zvol)])
                        runCommand(['dmsetup', 'resume', '/dev/mapper/%s-snap'%zvol])
                        cur.execute('UPDATE sync_queue SET started = 1 WHERE zvol = ?', [zvol])
                        con.commit()
                        self.logger.debug('Initial sync finished in %s'%(time.time()-start))

                    sync_status = runCommand(['dmsetup', 'status', '%s-snap'%zvol])
                    stats = re.findall(r"[\w-]+", sync_status[0])
                    if not (stats[4] == stats[6]):
                        self.logger.debug("Waiting for sync '%s' %s %s"%(sync_status[0], stats[4], stats[6]))
                    else:
                        cur.execute('DELETE FROM sync_queue WHERE zvol = ?', [zvol])
                        con.commit()

                        self.logger.debug('Reloaded local storage to zvol and temp %s in %s'%(runCommand(['dmsetup', 'status', '%s-snap'%zvol])[0], time.time()-start))
                        runCommand(['dmsetup', 'suspend', '/dev/mapper/%s-snap'%zvol])
                        runCommand(['dmsetup', 'reload', '/dev/mapper/%s-snap'%zvol, '--table', '0 %s linear /dev/zvol/%s/%s 0'%(devsize, self.ZPOOL, zvol)])
                        runCommand(['dmsetup', 'resume', '/dev/mapper/%s-snap'%zvol])
                        self.logger.debug('Synced local storage to local in %s'%(time.time()-start))
                        runCommand(['zfs', 'destroy', '%s/%s-temp-write'%(self.ZPOOL, zvol)])
                        self.disconnect_iscsi(target)

                        self.queue_connector.publish_message({'action': 'zvol_synced', 'zvol':zvol, 'status':'success'}, reply_to, correlation_id=correlation_id)
                        self.logger.debug("Sync time: %s"%(time.time() - start))
                except ActionError, msg:
                    cur.execute('DELETE FROM sync_queue WHERE zvol = ?', [zvol])
                    con.commit()

                    self.logger.exception('Error syncing %s: %s'%(zvol, str(msg)))
                    self.queue_connector.publish_message({'action': 'zvol_synced', 'zvol':zvol, 'status':'error', 'error':str(msg)}, reply_to, correlation_id=correlation_id)

        self.queue_connector._connection.add_timeout(self.SYNC_CHECK_TIMEOUT, self.run_sync)


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
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS sync_queue(zvol TEXT PRIMARY KEY NOT NULL, iscsi_target TEXT UNIQUE, devsize INT, reply_to TEXT, correlation_id TEXT, started BOOLEAN default 0, time INT)')
            con.commit()


        self.queue_connector = RabbitMQCommonClient('rocks.vm-manage', 'direct', self.process_message, lambda a: self.run_sync())
        self.queue_connector.run()

    def stop(self):
        self.queue_connector.stop()
        self.logger.info('RabbitMQ connector stopping called')


