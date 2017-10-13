#!/opt/rocks/bin/python
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
import imgstorage
from imgstoragedaemon import *
import NodeConfig
import logging

import time
import json
import random
import re
import signal
import sys
import os
import traceback
import rocks.db.helper
import rocks.util

from tornado.ioloop import IOLoop
from tornado.gen import Task, coroutine

from pysqlite2 import dbapi2 as sqlite3


#############
# SQLite TABLES Defined
#
# zvols:  zvol | zpool | iscsi_target | remotehost | remotepool | sync
# sync_queue:  zvol | zpool | remotehost | remotepool | is_sending  | is_delete_remote | time
#############
# msg formats
#
# Receive Messages:
#       map_zvol:  zpool, zvol, remotehost, remotepoool, sync
#       unmap_zvol:  zvol
#       list_dev:
#       list_initiator:
#       sync_zvol
#
# Send Messages:
#       zvol_mapped: nas, target, size, zvol, remotehost, remotepool, sync
#       zvol_unmapped: target, zvol
#       zvol_deleted:

def get_blk_dev_list():
    """ Return mappings of isci targets """
    try:
        out = runCommand(['iscsiadm', '-m', 'session', '-P3'])
        mappings = {}
        cur_target = None
        for line in out:
            if 'Target: ' in line:
                cur_target = line.split()[1]
            if 'Attached scsi disk ' in line:
                blockdev = line.split()[3] 
                mappings[cur_target] = blockdev
    except:
        return {}

    return mappings


def disconnect_iscsi(iscsi_target):
    return runCommand([
        'iscsiadm',
        '-m',
        'node',
        '-T',
        iscsi_target,
        '-u',
    ])


def get_zfs_list():
    """return a list of string containing all zfs file systems"""

    try:
        out = runCommand(['zfs', 'list', '-H'])
    except:
        return None

    fs = []
    for line in out:
        fs.append(line.split('\t')[0])
    return fs


class VmDaemon:

    def __init__(self):
        self.nc = NodeConfig.NodeConfig()
        self.NODE_NAME = self.nc.NODE_NAME
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/null'
        self.stderr_path = '/tmp/err.log'
        self.pidfile_path = '/var/run/img-storage-vm.pid'
        self.pidfile_timeout = 5
        self.function_dict = {
            'map_zvol': self.map_zvol,
            'unmap_zvol': self.unmap_zvol,
            'list_initiator': self.list_initiator,
            'list_dev': self.list_dev,
            'sync_zvol': self.sync_zvol,
        }
        self.logger = \
            logging.getLogger('imgstorage.imgstoragevm.VmDaemon')
        self.SQLITE_DB = '/opt/rocks/var/img_storage.db'

        self.SYNC_CHECK_TIMEOUT = 10

    def is_sync_enabled(self, zvol):
        """return True if a particular volume is supposed to sync """

        with sqlite3.connect(self.SQLITE_DB) as con:
            # print 'XXX is_sync_enabled zvol is ', zvol
            cur = con.cursor()
            cur.execute('SELECT sync FROM zvols WHERE zvol = ?', [zvol])
            sync = cur.fetchone()
            # print 'XXX is_sync_enabled sync is', sync
            if sync != None and sync[0] != 0:
                return True
        return False

    def is_sync_enabled_iscsi(self, target):
        """return True if a particular iscsi target is supposed to sync """

        with sqlite3.connect(self.SQLITE_DB) as con:
            # print 'XXX is_sync_enabled_target target is ', target
            cur = con.cursor()
            cur.execute(
                'SELECT sync FROM zvols WHERE iscsi_target = ?', [target])
            sync = cur.fetchone()
            # print 'XXX is_sync_enabled sync is', sync
            if sync != None and sync[0] != 0:
                return True
        return False

    def zpool(self, zvol):
        """returns pool on which a particular volume is located """
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('SELECT zpool FROM zvols WHERE zvol = ?', [zvol])
            row = cur.fetchone()
            return row[0]

        return None

    @coroutine
    def map_zvol(self, message, props):
        """ map a volume """
        self.logger.debug('Setting zvol %s' % message['target'])
        zvol = message.get('zvol')
        sync = message.get('sync')
        pool = message.get('remotepool')
        nas = message.get('nas')
        target = message.get('target')
        # print "XXX map_zvol(message)", message

        try:
            self.connect_iscsi(message['target'], message['nas'])

            # sometime get_blk_dev_list does not find the device because
            # the kernel hasn't populated the entry in sysfs so we need
            # this polling to make sure we avoid this latency

            count = 0
            while True:
                mappings = get_blk_dev_list()
                if message['target'] in mappings.keys():
                    break
                else:
                    if count == 3:
                        raise ActionError('Not found %s in targets'
                                          % message['target'])

                    # asynchronous time.sleep(1)

                    yield Task(IOLoop.instance().add_timeout,
                               time.time() + 1)
                    count += 1

            bdev = '/dev/%s' % mappings[message['target']]
            # print 'XXX map_zvol bdev is %s' % bdev

            # Record information about this volume. If the subsequent
            # volume creation fails, this is just stale information.

            with sqlite3.connect(self.SQLITE_DB) as con:
                cur = con.cursor()
                cur.execute('''INSERT OR REPLACE INTO 
                    zvols(zvol,zpool,nas,iscsi_target,sync) 
                    VALUES (?,?,?,?,?) '''
                            , (zvol, pool, nas, target, sync))
                con.commit()

            if sync:
                temp_size_cur = int(message['size'])
                if zvol and len(zvol) > 0:  # don't want to destroy the zpool
                    try:
                        runCommand(['zfs', 'destroy', '-r', '%s/%s'
                                    % (pool, zvol)])
                    except:
                        pass
                runCommand(zfs_create + ['-V', '%sgb'
                                         % message['size'], '%s/%s' % (pool,
                                                                       zvol)])
                runCommand(zfs_create + ['-V', '%sgb'
                                         % temp_size_cur, '-s', '%s/%s-temp-write'
                                         % (pool, zvol)])
                time.sleep(2)
                runCommand(['dmsetup', 'create', '%s-snap' % zvol,
                            '--table',
                            '0 %s snapshot %s /dev/zvol/%s/%s-temp-write P 16'
                            % (int(1024 ** 3 * temp_size_cur / 512),
                               bdev, pool, zvol)])
                bdev = '/dev/mapper/%s-snap' % zvol

            self.queue_connector.publish_message(json.dumps({
                'action': 'zvol_mapped',
                'target': message['target'],
                'bdev': bdev,
                'status': 'success',
            }), props.reply_to, reply_to=self.NODE_NAME,
                correlation_id=props.message_id)

            self.logger.debug('Successfully mapped %s to %s'
                              % (message['target'], bdev))

        except ActionError, msg:
            self.queue_connector.publish_message(json.dumps({
                'action': 'zvol_mapped',
                'target': message['target'],
                'status': 'error',
                'error': str(msg),
            }), props.reply_to, reply_to=self.NODE_NAME,
                correlation_id=props.message_id)
            self.logger.exception('Error mapping %s: %s'
                                  % (message['target'], str(msg)))
        except:
            self.logger.error('Unexpected exception (map_zvol).',
                              exc_info=True)
            self.queue_connector.publish_message(json.dumps({
                'action': 'zvol_unmapped',
                'target': message['target'],
                'zvol': zvol,
                'status': 'error',
                'error': 'unhandled exception in unmap_zvol',
            }), props.reply_to, reply_to=self.NODE_NAME,
                correlation_id=props.message_id)

    def list_initiator(self, message, properties):
        try:
            f = open("/etc/iscsi/initiatorname.iscsi")
            lines = filter(lambda x: 'InitiatorName' in x, 
                        [l.strip() for l in f.readlines()])
            name = lines[0].split('=')[-1] 
            self.queue_connector.publish_message(
                json.dumps({'action': 'zvol_list', 'status': 'success', 
                    'body': name}), 
                exchange='', routing_key=properties.reply_to)
        except:
            self.queue_connector.publish_message(
                json.dumps({'status': 'error', 'error': 'no initiator name'}), 
                exchange='', routing_key=properties.reply_to)
            
    def list_dev(self, message, props):
        mappings = self.get_dev_list()
        self.logger.debug('Got mappings %s' % mappings)
        self.queue_connector.publish_message(json.dumps({
            'action': 'dev_list',
            'status': 'success',
            'node_type': 'mixed',
            'body': mappings,
        }), exchange='', routing_key=props.reply_to)

    def get_dev_list(self):
        """ return of dictionary of information about various devices 
            Keys:  volume -- zvolume or generic iscsi (labeled volume<n>)
                   sync -- iscsi or sync, depending on type
                   target -- iscsi target 
                   device -- local device name
                   --- following keys are only for sync-type volumes
                   status
                   size
                   synced
                   bdev
                   started
                   time
        """
        mappings = {}
        bdev_mappings = get_blk_dev_list()
        bdev_mapped = []

        # Step 1. Find all the devices that are part of dmsetup (eg. some
        # state of sync
        try:
            out = runCommand(['dmsetup', 'status'])
        except:
            out = []
        if out[0] == 'No devices found':
            out = []
        for line in out:
            dev_ar = line.split()
            dev_name = (dev_ar[0])[:-1]
            zvol_name = re.search(r'([\w-]*)-snap', dev_name).group(1)
            mappings[zvol_name] = {'device': dev_name,
                                   'status': dev_ar[3],
                                   'size': int(dev_ar[2]) * 512 / 1024
                                   ** 3}
            mappings[zvol_name]['sync'] = 'sync'
            if dev_ar[3] != 'linear':
                mappings[zvol_name]['synced'] = '%s %s' % (dev_ar[4],
                                                           dev_ar[5])

            for target in bdev_mappings.keys():
                if target.endswith(zvol_name):
                    mappings[zvol_name]['target'] = target
                    mappings[zvol_name]['bdev'] = bdev_mappings[target]
                    bdev_mapped.append(target)

        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('SELECT zvol, started, time from sync_queue;')
            for row in cur.fetchall():
                (zvol, started, time) = row
                mappings[zvol]['started'] = started
                mappings[zvol]['time'] = time

        # Step 2. Want what is left-over (non-synced volumes, pure iSCSI)
        for t in bdev_mapped:
            try:
                del bdev_mappings[t]
            except:
                pass

        # Step 3.  Now go through any mappings left over and add
        mapidx = 0
        mkeys = bdev_mappings.keys()
        mkeys.sort()
        for target in mkeys:
            vname = "volume%d" % mapidx
            mappings[vname] = {'target': target,
                               'device': bdev_mappings[target],
                               'sync': ('sync' if self.is_sync_enabled_iscsi(target) else 'iscsi')}
            mapidx += 1
        return mappings

    def connect_iscsi(self, iscsi_target, node_name):
        connect_out = runCommand([
            'iscsiadm',
            '-m',
            'discovery',
            '-t',
            'sendtargets',
            '-p',
            node_name,
        ])
        self.logger.debug('Looking for target in iscsiadm output')
	cmdoutput = None
        for line in connect_out:
            parts = line.strip().split()
            self.logger.debug('discovery target: "%s"' % iscsi_target)
            self.logger.debug('discovery output: %s' % str(parts))
            self.logger.debug('discovery test: %s' % str(iscsi_target in parts))
            if iscsi_target in parts:  # has the target
                self.logger.debug('Found iscsi target in iscsiadm output')
                cmdoutput = runCommand([
                    'iscsiadm',
                    '-m',
                    'node',
                    '-T',
                    iscsi_target,
                    '-p',
                    node_name,
                    '-l'])
                self.logger.debug('iscsi login: %s' % str(cmdoutput))
                break
        if cmdoutput is None: 
            raise ActionError('Could not find iSCSI target %s on server %s'
                        % (iscsi_target, node_name))
        else:
            return cmdoutput

    def unmap_zvol(self, message, props):
        """ Received zvol unmap_zvol command from nas """
        zvol = message['zvol']
        # print 'XXX unmap zvol(message)', message
        # print 'XXX is_sync_enabled', self.is_sync_enabled(zvol)

        try:
            if self.is_sync_enabled(zvol):
                self.logger.debug('Tearing down zvol %s'
                                  % message['zvol'])
                while True:
                    if isFileUsed('/dev/mapper/%s-snap' % zvol):
                        time.sleep(0.1)
                        self.logger.debug('/dev/mapper/%s-snap is in use'
                                          % zvol)
                    else:
                        break
                runCommand(['dmsetup', 'remove', '--retry', '%s-snap'
                            % zvol])
                self.queue_connector.publish_message(json.dumps({
                    'action': 'zvol_unmapped',
                    'target': message['target'],
                    'zvol': zvol,
                    'status': 'success',
                }), props.reply_to, reply_to=self.NODE_NAME,
                    correlation_id=props.message_id)
            else:

                self.logger.debug('Tearing down target %s'
                                  % message['target'])
                mappings_map = get_blk_dev_list()
                if message['target'] not in mappings_map.keys() \
                        or disconnect_iscsi(message['target']):
                    self.queue_connector.publish_message(json.dumps({
                        'action': 'zvol_unmapped',
                        'target': message['target'],
                        'zvol': zvol,
                        'status': 'success',
                    }), props.reply_to, reply_to=self.NODE_NAME,
                        correlation_id=props.message_id)
        except ActionError, msg:

            self.queue_connector.publish_message(json.dumps({
                'action': 'zvol_unmapped',
                'target': message['target'],
                'zvol': zvol,
                'status': 'error',
                'error': str(msg),
            }), props.reply_to, reply_to=self.NODE_NAME,
                correlation_id=props.message_id)
            self.logger.error('Error unmapping %s: %s'
                              % (message['target'], str(msg)))

    def sync_zvol(self, message, props):
        zvol = message.get('zvol')
        target = message.get('target')

        mappings = get_blk_dev_list()
        try:
            if target not in mappings.keys():
                raise ActionError('Not found %s in targets' % target)

            devsize = runCommand(['blockdev', '--getsize', '/dev/%s'
                                  % mappings[target]])[0]

            with sqlite3.connect(self.SQLITE_DB) as con:
                cur = con.cursor()
                cur.execute('INSERT INTO sync_queue VALUES(?,?,?,?,?,0,?)', [
                    zvol,
                    target,
                    devsize,
                    props.reply_to,
                    props.message_id,
                    time.time(),
                ])
                self.logger.debug('Updated the db for zvol %s : %s'
                                  % (zvol, devsize))
                con.commit()
        except ActionError, msg:
            self.queue_connector.publish_message(json.dumps({
                'action': 'zvol_synced',
                'zvol': zvol,
                'status': 'error',
                'error': str(msg),
            }), props.reply_to, correlation_id=props.message_id)

            self.logger.exception('Error syncing %s: %s' % (zvol,
                                                            str(msg)))

    def run_sync(self):
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('''SELECT zvol, iscsi_target, 
                            devsize, reply_to,
                            correlation_id, started 
                            FROM sync_queue ORDER BY time ASC LIMIT 1'''
                        )
            row = cur.fetchone()
            if row:
                (
                    zvol,
                    target,
                    devsize,
                    reply_to,
                    correlation_id,
                    started,
                ) = row

                try:
                    start = time.time()
                    if not started:
                        self.logger.debug('Starting new sync %s' % zvol)
                        runCommand(['dmsetup', 'suspend',
                                    '/dev/mapper/%s-snap' % zvol])
                        runCommand(['dmsetup', 'reload',
                                    '/dev/mapper/%s-snap' % zvol,
                                    '--table',
                                    '0 %s snapshot-merge /dev/zvol/%s/%s /dev/zvol/%s/%s-temp-write P 16'
                                    % (devsize, self.zpool(zvol), zvol,
                                       self.zpool(zvol), zvol)])
                        runCommand(['dmsetup', 'resume',
                                    '/dev/mapper/%s-snap' % zvol])
                        cur.execute(
                            'UPDATE sync_queue SET started = 1 WHERE zvol = ?', [zvol])
                        con.commit()
                        self.logger.debug('Initial sync finished in %s'
                                          % (time.time() - start))

                    sync_status = runCommand(['dmsetup', 'status',
                                              '%s-snap' % zvol])
                    stats = re.findall(r"[\w-]+", sync_status[0])
                    if not stats[3] == stats[5]:
                        self.logger.debug("Waiting for sync '%s' %s %s"
                                          % (sync_status[0], stats[3], stats[5]))
                    else:
                        cur.execute(
                            'DELETE FROM sync_queue WHERE zvol = ?', [zvol])
                        con.commit()

                        self.logger.debug('Reloaded local storage to zvol and temp %s in %s'
                                          % (runCommand(['dmsetup', 'status',
                                                         '%s-snap' % zvol])[0], time.time()
                                             - start))
                        runCommand(['dmsetup', 'suspend',
                                    '/dev/mapper/%s-snap' % zvol])
                        runCommand(['dmsetup', 'reload',
                                    '/dev/mapper/%s-snap' % zvol,
                                    '--table',
                                    '0 %s linear /dev/zvol/%s/%s 0'
                                    % (devsize, self.zpool(zvol), zvol)])
                        runCommand(['dmsetup', 'resume',
                                    '/dev/mapper/%s-snap' % zvol])
                        self.logger.debug('Synced local storage to local in %s'
                                          % (time.time() - start))
                        runCommand(['zfs', 'destroy', '%s/%s-temp-write'
                                    % (self.zpool(zvol), zvol)])
                        disconnect_iscsi(target)

                        self.queue_connector.publish_message(json.dumps({'action': 'zvol_synced', 'zvol': zvol, 'status': 'success'}),
                                                             reply_to, correlation_id=correlation_id)
                        self.logger.debug('Sync time: %s'
                                          % (time.time() - start))
                except ActionError, msg:
                    cur.execute(
                        'DELETE FROM sync_queue WHERE zvol = ?', [zvol])
                    con.commit()

                    self.logger.exception('Error syncing %s: %s'
                                          % (zvol, str(msg)))
                    self.queue_connector.publish_message(json.dumps({
                        'action': 'zvol_synced',
                        'zvol': zvol,
                        'status': 'error',
                        'error': str(msg),
                    }), reply_to, correlation_id=correlation_id)

        self.queue_connector._connection.add_timeout(self.SYNC_CHECK_TIMEOUT,
                                                     self.run_sync)

    def process_message(self, props, message_str, deliver):
        message = json.loads(message_str)
        self.logger.debug('Received message %s' % message)
        if message['action'] not in self.function_dict.keys():
            self.queue_connector.publish_message(json.dumps({'status': 'error',
                                                             'error': 'action_unsupported'}), exchange='',
                                                 routing_key=props.reply_to)
            return

        try:
            self.function_dict[message['action']](message, props)
        except:
            self.logger.exception('Unexpected error: %s %s'
                                  % (sys.exc_info()[0],
                                     sys.exc_info()[1]))
            traceback.print_tb(sys.exc_info()[2])
            self.queue_connector.publish_message(json.dumps({'status': 'error',
                                                             'error': sys.exc_info()[1].message}), exchange='',
                                                 routing_key=props.reply_to,
                                                 correlation_id=props.message_id)

    def run(self):
        self.logger.debug('imgstoragevm starting')
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()

            cur.execute('''CREATE TABLE IF NOT EXISTS zvols(
                          zvol TEXT PRIMARY KEY NOT NULL,
                          zpool TEXT,
                          nas TEXT,
                          iscsi_target TEXT UNIQUE,
                          sync BOOLEAN)''')

            cur.execute('''CREATE TABLE IF NOT EXISTS sync_queue(
                                zvol TEXT PRIMARY KEY NOT NULL, 
                                iscsi_target TEXT UNIQUE, 
                                devsize INT, reply_to TEXT, 
                                correlation_id TEXT, 
                                started BOOLEAN default 0, time INT)'''
                        )
            con.commit()

        self.queue_connector = RabbitMQCommonClient('rocks.vm-manage',
                                                    'direct', "img-storage", "img-storage",
                                                    self.process_message, lambda a:
                                                    self.run_sync(),
                                                    routing_key=self.nc.NODE_NAME)
        self.queue_connector.run()

    def stop(self):
        self.queue_connector.stop()
        self.logger.info('RabbitMQ connector stopped')
