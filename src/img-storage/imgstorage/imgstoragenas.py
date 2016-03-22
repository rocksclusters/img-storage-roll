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

#############
# SQLite TABLES Defined
#
# zvols:  zvol | zpool | iscsi_target | remotehost | remotepool | sync
# zvolattrs: zvol | freq | nextsync | downspeed | upspeed
# globals: attr | value
# sync_queue:  zvol | zpool | remotehost | remotepool | is_sending  | is_delete_remote | time
# zvol_calls:  zvol |reply_to | time
#############
# msg formats
#
# Receive Messages:
#       map_zvol:  zpool, zvol, remotehost, remotepoool, sync
#       unmap_zvol:  zvol     XXX: really should have pool, too.
#       del_zvol: zvol, zpool
#       list_zvols:
#       set_zvol_attrs:
#       get_zvol_attrs:
#       get_attrs:
#       set_attrs:
#       del_attrs:
# Send Messages:
#       map_zvol: nas, target, size, zvol, remotehost, remotepool, sync
#       unmap_zvol: target, zvol
#       zvol_deleted:
#       zvol_list

from rabbitmqclient import RabbitMQCommonClient
from imgstorage import runCommand, ActionError, ZvolBusyActionError
import logging
import NodeConfig

import traceback
import imgstorage
from imgstoragedaemon import *

import time
import json

from multiprocessing.pool import ThreadPool

from pysqlite2 import dbapi2 as sqlite3
import sys
import signal
import pika
import socket
import rocks.util
import uuid

import subprocess

from tornado.gen import Task, Return, coroutine
import tornado.process


def get_iscsi_targets():
    """return a list of all the active target the dictionary keys
    are the target names and the data is their associated TID"""

    out = runCommand(['tgtadm', '--op', 'show', '--mode', 'target'])
    ret = []
    for line in out:
        if line.startswith('Target ') and len(line.split()) >= 2:
            ret.append(line.split()[2])
    return ret


STREAM = tornado.process.Subprocess.STREAM


@coroutine
def runCommandBackground(cmdlist, shell=False):
    """
    Wrapper around subprocess call using Tornado's Subprocess class.
    This routine can fork a process in the background without blocking the
    main IOloop, the the forked process can run for a long time without
    problem
    """

    LOG = logging.getLogger('imgstorage.imgstoragenas.NasDaemon')
    LOG.debug('Executing: ' + str(cmdlist))

    # tornado.process.initialize()

    sub_process = tornado.process.Subprocess(cmdlist, stdout=STREAM,
                                             stderr=STREAM, shell=shell)

    # we need to set_exit_callback to fetch the return value
    # the function can even be empty by it must be set or the
    # sub_process.returncode will be always None

    retval = 0
    sub_process.set_exit_callback(lambda value: value)

    (result, error) = \
        (yield [Task(sub_process.stdout.read_until_close),
                Task(sub_process.stderr.read_until_close)])

    if sub_process.returncode:
        raise ActionError('Error executing %s: %s' % (cmdlist, error))

    raise Return((result.splitlines(), error))


class NasDaemon:

    def __init__(self):
        # FIXME: user should be configurable at startup
        self.imgUser = 'img-storage'
        self.stdin_path = '/dev/null'
        self.stdout_path = '/tmp/out.log'
        self.stderr_path = '/tmp/err.log'
        self.pidfile_path = '/var/run/img-storage-nas.pid'
        self.pidfile_timeout = 5
        self.function_dict = {
            'map_zvol': self.map_zvol,
            'unmap_zvol': self.unmap_zvol,
            'zvol_mapped': self.zvol_mapped,
            'zvol_unmapped': self.zvol_unmapped,
            'list_zvols': self.list_zvols,
            'del_zvol': self.del_zvol,
            'get_zvol_attrs': self.get_zvol_attrs,
            'set_zvol_attrs': self.set_zvol_attrs,
            'get_attrs': self.get_attrs,
            'set_attrs': self.set_attrs,
            'del_attrs': self.del_attrs,
            'zvol_synced': self.zvol_synced,
        }

        self.nc = NodeConfig.NodeConfig()
        self.SQLITE_DB = '/opt/rocks/var/img_storage.db'
        self.NODE_NAME = self.nc.NODE_NAME
        self.ib_net = self.nc.SYNC_NETWORK

        self.sync_result = None

        self.results = {}
        if self.nc.IMG_SYNC_WORKERS:
            self.SYNC_WORKERS = int(self.nc.IMG_SYNC_WORKERS)
        else:
            self.SYNC_WORKERS = 5

        self.SYNC_CHECK_TIMEOUT = 10
        # try once/minute to add new jobs to the sync queue
        self.SYNC_PULL_TIMEOUT = 60
        # the default SYNC_PULL is 5 minutes
        self.SYNC_PULL_DEFAULT = 60 * 5

        self.logger = \
            logging.getLogger('imgstorage.imgstoragenas.NasDaemon')

        self.ZVOLATTRS = ['frequency', 'nextsync',
                          'downloadspeed', 'uploadspeed']

    def dbconnect(self):
        """ connect to sqlite3 database, turn on foreign constraints """
        con = sqlite3.connect(self.SQLITE_DB)
        cur = con.cursor()
        cur.execute('PRAGMA foreign_keys=ON')
        return con

    def getZvolAttr(self, zvol, attr=None):
        """ Return a single named attribute for a zvol, or a dictionary of
                attributes if attr=None """
        rval = None
        with self.dbconnect() as con:
            cur = con.cursor()
            if attr is not None:
                cur.execute("SELECT %s FROM zvolattrs WHERE zvol='%s'" %
                            (attr, zvol))
                row = cur.fetchone()
                if row != None:
                    rval = row[0]
            else:
                cur.execute("SELECT * from zvolattrs WHERE zvol='%s'" % zvol)
                row = cur.fetchone()
                if row != None:
                    rval = dict((cur.description[i][0], value)
                                for (i, value) in enumerate(row))

        return rval

    def setZvolAttr(self, zvol, attr, value=None):
        """ Set a single named attribute for a zvol. Set to Null of value is None """
        with self.dbconnect() as con:
            cur = con.cursor()
            cur.execute(
                'SELECT count(*) FROM zvolattrs WHERE zvol = ?', [zvol])
            if cur.fetchone()[0] == 0:
                cur.execute('INSERT INTO zvolattrs(zvol) VALUES(?)', [zvol])
            if value is None:
                setStmt = "SET %s=NULL" % attr
            elif isinstance(value, int):
                setStmt = "SET %s=%d" % (attr, value)
            else:
                setStmt = "SET %s='%s'" % (attr, value)
            cur.execute(""" UPDATE zvolattrs %s WHERE  zvol='%s'""" %
                        (setStmt, zvol))
            con.commit()

    def getAttr(self, attr=None):
        """ Return a single named global attribute or a dictionary of
                all attributes if attr=None. All values are type string """
        rval = None
        with self.dbconnect() as con:
            cur = con.cursor()
            if attr is not None:
                cur.execute("SELECT value FROM globals WHERE attr='%s'" %
                            (attr))
                row = cur.fetchone()
                if row != None:
                    rval = row[0]
            else:
                cur.execute("SELECT attr,value FROM globals")
                rval = {}
                for row in cur.fetchall():
                    attr, value = row
                    rval[attr] = value
        return rval

    def setAttr(self, attr, value=None):
        """ Set a single named attribute.Set to Null of value is None """
        with self.dbconnect() as con:
            if value is not None:
                value = str(value)
            cur = con.cursor()
            cur.execute('''INSERT OR REPLACE INTO globals(attr,value)
                               VALUES(?,?)''', [attr, value] )
            con.commit()

    def deleteAttr(self, attr):
        """ delete single named attribute """
        with self.dbconnect() as con:
            cur = con.cursor()
            cur.execute('''DELETE FROM globals WHERE attr="%s"''' % attr )
            con.commit()

    # Attribute get/set messages

    def get_zvol_attrs(self, message, props):
        #  input: get_zvol_attr messages
        #  output:  get_zvol_attr message --> requestor

        zvol = message['zvol']
        try:
            attrs = self.getZvolAttr(zvol)
            attrs['zvol'] = zvol
        except Exception as err:
            self.failAction(props.reply_to, 'get_zvol_attrs', str(err))
            return
        reply = json.dumps({'action': 'get_zvol_attrs', 'status': 'success',
                            'body': attrs})
        self.queue_connector.publish_message(reply, exchange='',
                                             routing_key=props.reply_to)

    def set_zvol_attrs(self, message, props):
        #  input: set_zvol_attr message
        #  output:  set_zvol_attr message --> requestor
        #  state updates: zvols attr table

        zvol = message['zvol']
        try:
            for k in message.keys():
                if k in self.ZVOLATTRS:
                    self.setZvolAttr(zvol, k, message[k])
        except Exception as err:
            self.failAction(props.reply_to, 'set_zvol_attrs', str(err))
            return

        reply = json.dumps({'action': 'set_zvol_attrs', 'status': 'success'})
        self.queue_connector.publish_message(reply, exchange='',
                                             routing_key=props.reply_to)

    def get_attrs(self, message, props):
        #  input: get_attrs messages
        #  output:  get_attrs message --> requestor

        try:
            attrs = self.getAttr()
        except Exception as err:
            self.failAction(props.reply_to, 'get_attrs', str(err))
            return
        reply = json.dumps({'action': 'get_attrs', 'status': 'success',
                            'attrs': attrs})
        self.queue_connector.publish_message(reply, exchange='',
                                             routing_key=props.reply_to)

    def set_attrs(self, message, props):
        #  input: set_attrs message
        #  output:  set_attrs message --> requestor
        #  state updates: attrs table

        try:
            attrs = message['attrs']
            for k in attrs.keys():
                self.setAttr(k, attrs[k])
        except Exception as err:
            self.failAction(props.reply_to, 'set_attrs', str(err))
            return

        reply = json.dumps({'action': 'set_attrs', 'status': 'success'})
        self.queue_connector.publish_message(reply, exchange='',
                                             routing_key=props.reply_to)

    def del_attrs(self, message, props):
        #  input: del_attrs message
        #  output:  del_attrs message --> requestor
        #  state updates: attrs table

        try:
            for attr in message['attrs']:
                self.deleteAttr(attr)
        except Exception as err:
            self.failAction(props.reply_to, 'del_attrs', str(err))
            return

        reply = json.dumps({'action': 'del_attrs', 'status': 'success'})
        self.queue_connector.publish_message(reply, exchange='',
                                             routing_key=props.reply_to)

    ############   Main Run Method ###################

    def run(self):
        self.pool = ThreadPool(processes=self.SYNC_WORKERS)
        with self.dbconnect() as con:
            cur = con.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS zvol_calls(
                          zvol TEXT PRIMARY KEY NOT NULL,
                          reply_to TEXT NOT NULL,
                          time INT NOT NULL)''')
            cur.execute('''CREATE TABLE IF NOT EXISTS zvols(
                          zvol TEXT PRIMARY KEY NOT NULL,
                          zpool TEXT,
                          iscsi_target TEXT UNIQUE,
                          remotehost TEXT,
                          remotepool TEXT,
                          sync BOOLEAN)''')
            cur.execute('''CREATE TABLE IF NOT EXISTS zvolattrs(
                          zvol TEXT PRIMARY KEY NOT NULL,
                          frequency INT DEFAULT NULL,
                          nextsync INT DEFAULT NULL,
                          downloadspeed INT DEFAULT NULL,
                          uploadspeed INT DEFAULT NULL)''')
            cur.execute('''CREATE TABLE IF NOT EXISTS sync_queue(
                          zvol TEXT PRIMARY KEY NOT NULL,
                          zpool TEXT NOT NULL,
                          remotehost TEXT,
                          remotepool TEXT,
                          is_sending BOOLEAN,
                          is_delete_remote BOOLEAN,
                          time INT)''')
            cur.execute('''CREATE TABLE IF NOT EXISTS globals(
                          attr TEXT PRIMARY KEY NOT NULL,
                          value TEXT)''')
            cur.execute('DELETE FROM sync_queue')
            con.commit()
            # Record all parameters in the configuration file in globals
            for key in self.nc.DATA.keys():
                self.setAttr(key, self.nc.DATA[key])
            if self.getAttr('frequency') is None:
                self.setAttr('frequency', self.SYNC_PULL_DEFAULT)

        self.queue_connector = RabbitMQCommonClient('rocks.vm-manage',
                                                    'direct', "img-storage", "img-storage",
                                                    self.process_message, lambda a:
                                                    self.startup(),
                                                    routing_key=self.nc.NODE_NAME)
        self.queue_connector.run()

    def failAction(
        self,
        routing_key,
        action,
        error_message,
    ):
        if routing_key != None and action != None:
            self.queue_connector.publish_message(json.dumps({'action': action,
                                                             'status': 'error', 'error': error_message}),
                                                 exchange='', routing_key=routing_key)
        self.logger.error('Failed %s: %s' % (action, error_message))

    def startup(self):
        self.schedule_zvols_pull()
        self.schedule_next_sync()

    @coroutine
    def map_zvol(self, message, props):
        #  input: map_zvol message
        #  output:  map_zvol message --> remotehost
        #  state updates: zvol table

        remotehost = message['remotehost']
        remotepool = message['remotepool']
        zpool_name = message['zpool']
        zvol_name = message['zvol']
        sync = message['sync']

        # print "XXX map_zvol (message): ", message
        self.logger.debug('Setting zvol %s' % zvol_name)

        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            try:
                self.lock_zvol(zvol_name, props.reply_to)
                cur.execute(
                    'SELECT count(*) FROM zvols WHERE zvol = ?', [zvol_name])

                volume = "%s/%s" % (zpool_name, zvol_name)
                if cur.fetchone()[0] == 0:

                    """ Create a zvol, if it doesn't already exist """
                    # check if volume already exists
                    # print 'XXX checking if  zvol %s exists' % volume
                    self.logger.debug('checking if  zvol %s exists' % volume)
                    rcode = subprocess.call(["zfs", "list", volume])
                    # print 'XXX check complete (%s)' % volume
                    self.logger.debug('check complete (%s)' % volume)
                    if rcode != 0:
                        # create the zfs FS
                        yield runCommandBackground(zfs_create
                                                   + ['-V', '%sgb' % message['size'], volume])
                        self.logger.debug('Created new zvol %s' % volume)
                    else:
                        self.logger.debug('Vol %s exists' % volume)

                    # Record the creation of the volume
                    cur.execute('''INSERT OR REPLACE INTO 
                                zvols(zvol,zpool,iscsi_target,remotehost,remotepool,sync) 
                                VALUES (?,?,?,?,?,?) '''
                                , (zvol_name, zpool_name, None, None, None, False))
                    con.commit()
                    # print 'XXX Created new zvol %s' % volume
                    self.logger.debug('Created new zvol %s' % volume)

                cur.execute(
                    'SELECT remotehost FROM zvols WHERE zvol = ?', [zvol_name])
                row = cur.fetchone()
                if row != None and row[0] != None:  # zvol is mapped
                    raise ActionError(
                        'Error when mapping zvol: already mapped')

                ip = None
                use_ib = False

                if self.ib_net:
                    try:
                        ip = socket.gethostbyname('%s.%s'
                                                  % (remotehost, self.ib_net))
                        use_ib = True
                    except:
                        pass

                if not use_ib:
                    try:
                        ip = socket.gethostbyname(remotehost)
                    except:
                        raise ActionError('Host %s is unknown'
                                          % remotehost)

                iscsi_target = ''
                (out, err) = (yield runCommandBackground([
                    '/opt/rocks/bin/tgt-setup-lun-lock',
                    '-n',
                    zvol_name,
                    '-d',
                    '/dev/%s/%s' % (zpool_name, zvol_name),
                    ip,
                ]))

                for line in out:
                    if 'Creating new target' in line:
                        start = 'Creating new target (name='.__len__()
                        iscsi_target = line[start:line.index(',')]

                # print 'XXX Mapped %s to iscsi target %s' % (zvol_name,
                # iscsi_target)
                self.logger.debug('Mapped %s to iscsi target %s'
                                  % (zvol_name, iscsi_target))

                # Update the Zvols table with the target, remote and sync
                # attributes
                cur.execute('''INSERT OR REPLACE INTO 
                            zvols(zvol,zpool,iscsi_target,remotehost, remotepool,sync) 
                            VALUES (?,?,?,?,?,?) '''
                            , (zvol_name, zpool_name, iscsi_target, remotehost, remotepool, sync))
                con.commit()

                # print 'XXX iscsi target %s inserted into DB' % iscsi_target

                def failDeliver(
                    target,
                    zvol,
                    reply_to,
                    remotehost,
                ):
                    self.detach_target(target, True)
                    self.failAction(props.reply_to, 'zvol_mapped',
                                    'Compute node %s is unavailable'
                                    % remotehost)
                    self.release_zvol(zvol_name)

                # Send a map_zvol message to remotehost
                # XXX: Should rename this message
                self.queue_connector.publish_message(json.dumps({
                    'action': 'map_zvol',
                    'target': iscsi_target,
                    'nas': ('%s.%s' % (self.NODE_NAME,
                                       self.ib_net) if use_ib else self.NODE_NAME),
                    'size': message['size'],
                    'zvol': zvol_name,
                    'sync': sync,
                    'remotehost': remotehost,
                    'remotepool': remotepool,
                }), remotehost, self.NODE_NAME, on_fail=lambda:
                    failDeliver(iscsi_target, zvol_name,
                                props.reply_to, remotehost))
                self.logger.debug('Setting iscsi %s sent'
                                  % iscsi_target)
            except ActionError, err:
                if not isinstance(err, ZvolBusyActionError):
                    self.release_zvol(zvol_name)
                self.failAction(props.reply_to, 'zvol_mapped', str(err))

    def unmap_zvol(self, message, props):
        #  input: unmap_zvol message
        #  output:  unmap_zvol message --> remotehost
        #  state updates: None

        zvol_name = message['zvol']
        self.logger.debug('Tearing down zvol %s' % zvol_name)

        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            try:
                cur.execute(
                    'SELECT remotehost, iscsi_target FROM zvols WHERE zvol = ?', [zvol_name])
                row = cur.fetchone()
                if row == None:
                    raise ActionError('ZVol %s not found in database'
                                      % zvol_name)
                (remotehost, target) = row
                if remotehost == None:
                    raise ActionError('ZVol %s is not mapped'
                                      % zvol_name)

                self.lock_zvol(zvol_name, props.reply_to)
                self.queue_connector.publish_message(json.dumps({'action': 'unmap_zvol', 'target': target, 'zvol': zvol_name}),
                                                     remotehost, self.NODE_NAME, on_fail=lambda:
                                                     self.failAction(props.reply_to, 'zvol_unmapped', 'Compute node %s is unavailable'
                                                                     % remotehost))
                self.logger.debug('Tearing down zvol %s sent'
                                  % zvol_name)
            except ActionError, err:

                if not isinstance(err, ZvolBusyActionError):
                    self.release_zvol(zvol_name)
                    self.failAction(props.reply_to, 'zvol_unmapped',
                                    str(err))
                else:
                    return False

    @coroutine
    def del_zvol(self, message, props):
        #  input: del_zvol message
        #  output:  zvol_deleted message --> requestor
        #  state updates: zvols table, Entry deleted

        zvol_name = message['zvol']
        zpool_name = message['zpool']
        self.logger.debug('Deleting zvol %s' % zvol_name)
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            try:
                self.lock_zvol(zvol_name, props.reply_to)
                cur.execute(
                    'SELECT remotehost, iscsi_target FROM zvols WHERE zvol = ?', [zvol_name])
                row = cur.fetchone()
                if row == None:
                    raise ActionError('ZVol %s not found in database'
                                      % zvol_name)
                if row[0] != None:
                    raise ActionError('Error deleting zvol %s: is mapped'
                                      % zvol_name)

                self.logger.debug('Invoking zfs destroy %s/%s'
                                  % (zpool_name, zvol_name))
                yield runCommandBackground(['zfs', 'destroy', '%s/%s'
                                            % (zpool_name, zvol_name), '-r'])
                self.logger.debug('zfs destroy success %s' % zvol_name)

                cur.execute('DELETE FROM zvolattrs WHERE zvol = ?',
                            [zvol_name])
                cur.execute('DELETE FROM zvols WHERE zvol = ?',
                            [zvol_name])
                con.commit()

                self.release_zvol(zvol_name)
                self.queue_connector.publish_message(json.dumps({'action': 'zvol_deleted', 'status': 'success'}), exchange='',
                                                     routing_key=props.reply_to)
            except ActionError, err:
                if not isinstance(err, ZvolBusyActionError):
                    self.release_zvol(zvol_name)
                self.failAction(props.reply_to, 'zvol_deleted',
                                str(err))

    def zvol_mapped(self, message, props):
        target = message['target']

        zvol = None
        reply_to = None

        self.logger.debug('Got zvol mapped message %s' % target)
        with sqlite3.connect(self.SQLITE_DB) as con:
            try:
                cur = con.cursor()
                cur.execute('''SELECT zvol_calls.reply_to,
                        zvol_calls.zvol, zvols.zpool, zvols.sync FROM zvol_calls
                        JOIN zvols ON zvol_calls.zvol = zvols.zvol
                        WHERE zvols.iscsi_target = ?''',
                            [target])
                (reply_to, zvol, zpool, sync) = cur.fetchone()

                if message['status'] != 'success':
                    raise ActionError('Error attaching iSCSI target to compute node: %s'
                                      % message.get('error'))

                if not sync:
                    self.release_zvol(zvol)
                else:
                    cur.execute(
                        'DELETE FROM sync_queue WHERE zvol = ?', [zvol])
                    cur.execute('''INSERT INTO 
                                   sync_queue(zvol,zpool,remotehost,is_sending,is_delete_remote,time,remotepool)
                                    SELECT zvol,?,?,1,1,?,remotepool 
                                    FROM zvols 
                                    WHERE iscsi_target = ? '''
                                , [zpool, props.reply_to, time.time(), target])
                    con.commit()

                self.queue_connector.publish_message(json.dumps({'action': 'zvol_mapped', 'bdev': message['bdev'], 'status': 'success'
                                                                 }), exchange='', routing_key=reply_to)
            except ActionError, err:
                self.release_zvol(zvol)
                self.failAction(reply_to, 'zvol_mapped', str(err))

    def zvol_unmapped(self, message, props):
        target = message['target']
        zvol = message['zvol']
        self.logger.debug('Got zvol %s unmapped message' % target)

        reply_to = None

        try:
            with sqlite3.connect(self.SQLITE_DB) as con:
                cur = con.cursor()

                # get request destination

                cur.execute('''SELECT reply_to, zpool, remotepool, sync 
                                FROM zvol_calls 
                                JOIN zvols 
                                ON zvol_calls.zvol = zvols.zvol 
                                WHERE zvols.zvol = ?'''
                            , [zvol])
                [reply_to, zpool, remotepool, sync] = cur.fetchone()

                if message['status'] == 'error':
                    raise ActionError('Error detaching iSCSI target from compute node: %s'
                                      % message.get('error'))

                if not sync:
                    self.detach_target(target, True)
                    self.release_zvol(zvol)
                else:
                    self.detach_target(target, False)
                    cur.execute(
                        'UPDATE sync_queue SET is_delete_remote = 1 WHERE zvol = ?', [zvol])
                    if cur.rowcount == 0:
                        cur.execute('''INSERT INTO 
                            sync_queue(zvol,zpool,remotehost,remotepool,is_sending,
                            is_delete_remote, time)  
                            VALUES(?,?,?,?,0,1,?)''',
                                    [zvol, zpool, props.reply_to, remotepool,
                                     time.time()])
                    con.commit()

                self.queue_connector.publish_message(json.dumps({'action': 'zvol_unmapped', 'status': 'success'}), exchange='',
                                                     routing_key=reply_to)
        except ActionError, err:

            self.release_zvol(zvol)
            self.failAction(reply_to, 'zvol_unmapped', str(err))

    def zvol_synced(self, message, props):
        zvol = message['zvol']
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute(
                'SELECT iscsi_target FROM zvols WHERE zvol = ?', [zvol])
            [target, ] = cur.fetchone()
            self.detach_target(target, False)
            self.release_zvol(zvol)

    @coroutine
    def schedule_next_sync(self):
        try:
            with self.dbconnect() as con:
                cur = con.cursor()

                for (zvol, job_result) in self.results.items():
                    if job_result.ready():
                        del self.results[zvol]
                        cur.execute('''SELECT 
                                       remotehost, is_sending, zvol, zpool, is_delete_remote,remotepool
                                       FROM sync_queue 
                                       WHERE zvol = ?'''
                                    , [zvol])
                        row = cur.fetchone()

                        try:
                            if not row:
                                raise ActionError('Not found record for %s in sync_queue table'
                                                  % zvol)

                            (remotehost, is_sending, zvol, zpool,
                             is_delete_remote, remotepool) = row

                            self.logger.debug('Sync %s is ready' % zvol)

                            job_result.get()  # will raise exception is there was one during job execution
                            if is_sending:
                                cur.execute(
                                    'SELECT iscsi_target FROM zvols WHERE zvol = ?', [zvol])
                                target = cur.fetchone()[0]
                                self.queue_connector.publish_message(json.dumps({'action': 'sync_zvol', 'zvol': zvol,
                                                                                 'target': target}), remotehost,
                                                                     self.NODE_NAME,
                                                                     on_fail=lambda:
                                                                     self.logger.error('Compute node %s is unavailable to sync zvol %s'
                                                                                       % (remotehost, zvol)))  # reply back to compute node
                            elif is_delete_remote:
                                cur.execute(
                                    'UPDATE zvols SET remotehost = NULL, remotepool = NULL where zvol = ?', [zvol])
                                con.commit()
                                self.release_zvol(zvol)
                        except ActionError, msg:

                            self.logger.exception('Error performing sync for %s: %s'
                                                  % (zvol, str(msg)))
                        finally:
                            cur.execute(
                                'DELETE FROM sync_queue WHERE zvol = ?', [zvol])
                            con.commit()

                for row in \
                    cur.execute('''SELECT remotehost, is_sending, zvol, 
                                    zpool, is_delete_remote, remotepool 
                                    FROM sync_queue 
                                    ORDER BY time ASC'''
                                ):
                    (remotehost, is_sending, zvol, zpool,
                     is_delete_remote, remotepool) = row
                    self.logger.debug('Have sync job %s' % zvol)

                    if self.ib_net:
                        remotehost += '.%s' % self.ib_net

                    if not self.results.get(zvol) and len(self.results) \
                            < self.SYNC_WORKERS:
                        self.logger.debug('Starting new sync %s' % zvol)
                        if is_sending:
                            self.results[zvol] = \
                                self.pool.apply_async(self.upload_snapshot,
                                                      [zpool, zvol, remotehost, remotepool])
                        else:
                            self.results[zvol] = \
                                self.pool.apply_async(self.download_snapshot,
                                                      [zpool, zvol, remotehost,  remotepool, is_delete_remote])

                self.queue_connector._connection.add_timeout(self.SYNC_CHECK_TIMEOUT,
                                                             self.schedule_next_sync)
        except:
            self.logger.error('Exception in schedule_next_sync',
                              exc_info=True)

    def schedule_zvols_pull(self):

        # self.logger.debug("Scheduling new pull jobs")

        with sqlite3.connect(self.SQLITE_DB) as con:
            try:
                cur = con.cursor()
                now = int(time.time())
                # Select zvols whose time has come to sync
                # if a nextsync has never been set, set one
                cur.execute('''SELECT z.zvol, z.zpool, z.remotehost, z.remotepool,
                               za.frequency,za.nextsync 
                                FROM zvols z LEFT JOIN zvolattrs za ON
                                z.zvol=za.zvol
                                WHERE iscsi_target IS NULL 
                                AND remotehost IS NOT NULL 
                                AND (nextsync is NULL  OR nextsync < %d)
                                ORDER BY nextsync ASC, z.zvol DESC;''' % now
                            )
                rows = cur.fetchall()
                for row in rows:
                    (zvol, zpool, remotehost, remotepool, frequency, nextsync) = row
                    delta = self.getZvolAttr(zvol, 'frequency')
                    delta = int(self.getAttr('frequency')
                                ) if delta is None else delta
                    delta = self.SYNC_PULL_DEFAULT if delta is None else delta

                    # add this one to the sync queue, if it isn't already there
                    cur.execute('INSERT or IGNORE INTO sync_queue VALUES(?,?,?,?,0,0,?)', [zvol, zpool, remotehost, remotepool,
                                                                                           time.time()])
                    con.commit()
                    # when we should schedule again for this zvol
                    self.setZvolAttr(zvol, 'nextsync', now + delta)

            except Exception, ex:
                self.logger.exception(ex)

        self.queue_connector._connection.add_timeout(self.SYNC_PULL_TIMEOUT,
                                                     self.schedule_zvols_pull)

    def upload_snapshot(
        self,
        zpool,
        zvol,
        remotehost,
        remotezpool,
        ):
        args = ['/opt/rocks/bin/snapshot_upload.sh', 
                '-p', zpool, 
                '-v', zvol, 
                '-r', remotehost,
                '-y', remotezpool,
                '-u', self.imgUser]
        upload_speed = self.getZvolAttr(zvol,'uploadspeed')
        if(not upload_speed):
            upload_speed = self.getAttr('uploadspeed')
        if(upload_speed):
            args.extend(['-t', upload_speed])
        runCommand(args)

    def download_snapshot(
        self,
        zpool,
        zvol,
        remotehost,
        remotezpool,
        is_delete_remote,
        ):
        args = ['/opt/rocks/bin/snapshot_download.sh', 
                    '-p', zpool, 
                    '-v', zvol, 
                    '-r', remotehost,
                    '-y', remotezpool,
                    '-u', self.imgUser]
        if(is_delete_remote):
            args.append('-d')

        download_speed = self.getZvolAttr(zvol,'downloadspeed')
        if(not download_speed):
            download_speed = self.getAttr('downloadspeed')
        if(download_speed and not is_delete_remote):
            args.extend(['-t', download_speed])

        runCommand(args)
        
    def detach_target(self, target, is_remove_host):
        if target:
            tgt_num = self.find_iscsi_target_num(target)
            if tgt_num:

                # this cammand is also run be the rocks clean host storagemap
                # which is not inside an IOLoop for the moment do not use coroutine
                # TODO find a solution for this

                runCommand([  # remove iscsi target
                    'tgtadm',
                    '--lld',
                    'iscsi',
                    '--op',
                    'delete',
                    '--mode',
                    'target',
                    '--tid',
                    tgt_num,
                ])

        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            if is_remove_host:
                cur.execute('''UPDATE zvols 
                                SET iscsi_target = NULL, 
                                remotehost = NULL, 
                                remotepool = NULL 
                                where iscsi_target = ?'''
                            , [target])
            else:
                cur.execute('''UPDATE zvols 
                                SET iscsi_target = NULL 
                                where iscsi_target = ?'''
                            , [target])
            con.commit()

    def clear_zvols_table(self, zvol):
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('''UPDATE zvols SET iscsi_target = NULL,
                    remotehost = NULL, remotepool = NULL
                    where zvol = ?''',
                        [zvol])
            con.commit()

    def list_zvols(self, message, properties):
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('''SELECT z.zvol, z.zpool, z.iscsi_target, z.remotehost, 
                           z.remotepool, s.is_sending, s.is_delete_remote, s.time, 
                           za.nextsync, za.frequency, zc.zvol as locked FROM 
                           zvols z LEFT JOIN zvolattrs za ON z.zvol=za.zvol 
                           LEFT JOIN sync_queue s on z.zvol=s.zvol 
                           LEFT JOIN zvol_calls zc ON z.zvol=zc.zvol'''
                        )
            r = [dict((cur.description[i][0], value) for (i, value) in
                      enumerate(row)) for row in cur.fetchall()]
            self.queue_connector.publish_message(json.dumps({'action': 'zvol_list', 'status': 'success', 'body': r}), exchange='',
                                                 routing_key=properties.reply_to)

    def process_message(self, properties, message_str, deliver):

        # self.logger.debug("Received message %s"%message)
        message = json.loads(message_str)
        if message['action'] not in self.function_dict.keys():
            self.queue_connector.publish_message(json.dumps({'status': 'error',
                                                             'error': 'action_unsupported'}), exchange='',
                                                 routing_key=properties.reply_to)
            return

        try:
            return self.function_dict[message['action']](message,
                                                         properties)
        except:
            self.logger.exception('Unexpected error: %s %s'
                                  % (sys.exc_info()[0],
                                     sys.exc_info()[1]))
            if properties.reply_to:
                self.queue_connector.publish_message(json.dumps({'status': 'error', 'error': sys.exc_info()[1].message}),
                                                     exchange='', routing_key=properties.reply_to)

    def stop(self):
        self.queue_connector.stop()
        self.logger.info('RabbitMQ connector stopping called')

    def lock_zvol(self, zvol_name, reply_to):
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            try:
                cur.execute('INSERT INTO zvol_calls VALUES (?,?,?)',
                            (zvol_name, reply_to, time.time()))
                con.commit()
            except sqlite3.IntegrityError:
                raise ZvolBusyActionError('ZVol %s is busy' % zvol_name)

    def release_zvol(self, zvol):
        with sqlite3.connect(self.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('DELETE FROM zvol_calls WHERE zvol = ?', [zvol])
            con.commit()

    def find_iscsi_target_num(self, target):
        out = runCommand(['tgtadm', '--op', 'show', '--mode', 'target'])
        for line in out:
            if line.startswith('Target ') and line.split()[2] == target:
                return (line.split()[1])[:-1]
        return None
