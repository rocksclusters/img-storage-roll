#!/opt/rocks/bin/python

import sys, os
lib_path = os.path.abspath('src/img-storage')
sys.path.insert(1, lib_path)

import unittest
from mock import MagicMock, ANY
import mock
from imgstorage.imgstoragenas import NasDaemon
from imgstorage.rabbitmqclient import RabbitMQCommonClient

import uuid
import time

from pysqlite2 import dbapi2 as sqlite3

from pika.spec import BasicProperties
from StringIO import StringIO

import datetime

class TestSyncFunctions(unittest.TestCase):

    def mock_rabbitcli(self, exchange, exchange_type, process_message=None):
        class MockRabbitMQCommonClient(RabbitMQCommonClient):
            def publish_message(self, message, routing_key=None, reply_to=None, exchange=None, correlation_id=None, on_fail=None):
                return
        return MockRabbitMQCommonClient

    @mock.patch('imgstorage.imgstoragenas.RabbitMQCommonClient')
    @mock.patch('imgstorage.imgstoragevm.RabbitMQCommonClient')
    def setUp(self, mock_rabbit_vm, mock_rabbit_sync):
        self.nas_client = NasDaemon()
        mock_rabbit_vm.publish_message = MagicMock()
        mock_rabbit_sync.publish_message = MagicMock()
        self.nas_client.process_message = MagicMock()

        self.nas_client.is_sync_node = MagicMock(return_value = True)

        self.nas_client.SQLITE_DB = '/tmp/test_db_%s'%uuid.uuid4()
        self.nas_client.run()

        with sqlite3.connect(self.nas_client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS zvol_calls(zvol TEXT PRIMARY KEY NOT NULL, reply_to TEXT NOT NULL, time INT NOT NULL)')
            cur.execute('CREATE TABLE IF NOT EXISTS zvols(zvol TEXT PRIMARY KEY NOT NULL, iscsi_target TEXT UNIQUE, remotehost TEXT)')
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol1', None, None))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol2', None, 'compute-0-1'))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol3_busy', None, 'compute-0-1'))
            cur.execute('INSERT INTO zvol_calls VALUES (?,?,?)',('vol3_busy', 'reply_to', time.time()))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol4_busy', 'iqn.2001-04.com.nas-0-1-vol4_busy', 'compute-0-1'))
            cur.execute('INSERT INTO zvol_calls VALUES (?,?,?)',('vol4_busy', 'reply_to', time.time()))
            con.commit()

    def tearDown(self):
        os.remove(self.nas_client.SQLITE_DB)


    @mock.patch('imgstorage.imgstoragenas.NasDaemon.download_snapshot')
    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_zvol_unmapped_success(self, mock_run_command, mock_download_snapshot):
        zvol = 'vol4_busy'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        mock_run_command.return_value = (iscsiadm_session_response%(target, zvol)).splitlines()
        self.nas_client.zvol_unmapped(
            {'action': 'zvol_unmapped', 'target':target, 'zvol': zvol, 'status':'success'},
            BasicProperties(reply_to='compute-0-3', correlation_id='message_id'))

        self.nas_client.schedule_next_sync()
        self.nas_client.pool.close()
        self.nas_client.pool.join()
        print mock_download_snapshot.mock_calls
        self.nas_client.download_snapshot.assert_called_with(zvol, 'compute-0-3.ibnet', 1)
        with sqlite3.connect(self.nas_client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('SELECT zvol, iscsi_target, remotehost FROM zvols WHERE zvol = ?',[zvol])
            self.assertSequenceEqual(cur.fetchone(), [zvol, None, 'compute-0-1'])



    @mock.patch('imgstorage.imgstoragevm.VmDaemon.is_sync_enabled', return_value=True)
    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_zvol_synced_success(self, mock_run_command, mock_sync_enabled):
        zvol = 'vol2'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        mock_run_command.return_value = (iscsiadm_session_response%(target, zvol)).splitlines()
        self.nas_client.zvol_synced(
            {'action': 'zvol_synced', 'status':'success', 'zvol':zvol},
            BasicProperties(reply_to='reply_to', correlation_id='message_id'))
        self.assertFalse(self.check_zvol_busy(zvol))

        with sqlite3.connect(self.nas_client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('SELECT zvol, iscsi_target, remotehost FROM zvols WHERE zvol = ?',[zvol])
            self.assertSequenceEqual(cur.fetchone(), [zvol, None, 'compute-0-1'])

    @mock.patch('imgstorage.imgstoragenas.runCommand')
    @mock.patch('imgstorage.imgstoragenas.NasDaemon.upload_snapshot')
    def test_zvol_mapped(self, mock_upload_snapshot, mock_run_command):
        zvol = 'vol4_busy'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        bdev = 'sdc'
        mock_run_command.return_value = (iscsiadm_session_response%(target, zvol)).splitlines()
        self.nas_client.zvol_mapped(
            {'action': 'zvol_mapped', 'target':target, 'bdev':bdev, 'zvol':zvol, 'status':'success'},
            BasicProperties(reply_to='reply_to', correlation_id='message_id'))
        self.nas_client.schedule_next_sync()
        self.nas_client.pool.close()
        self.nas_client.pool.join()
        print mock_upload_snapshot.mock_calls
        mock_upload_snapshot.assert_called_with(zvol, 'reply_to.ibnet')
        self.assertTrue(self.check_zvol_busy(zvol))
        with sqlite3.connect(self.nas_client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('SELECT zvol, iscsi_target, remotehost FROM zvols WHERE zvol = ?',[zvol])
            self.assertSequenceEqual(cur.fetchone(), [zvol, target, 'compute-0-1'])




    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_zvol_mapped_finished(self, mock_run_command):
        zvol = 'vol4_busy'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        bdev = 'sdc'
        mock_run_command.return_value = (iscsiadm_session_response%(target, zvol)).splitlines()
        self.nas_client.zvol_mapped(
            {'action': 'zvol_mapped', 'target':target, 'bdev':bdev, 'zvol':zvol, 'status':'success'},
            BasicProperties(reply_to='reply_to', correlation_id='message_id'))
        self.nas_client.sync_result = MagicMock()
        self.nas_client.sync_result.ready = MagicMock(return_value=True)
        self.nas_client.schedule_next_sync()

        self.nas_client.queue_connector.publish_message.assert_called_with(
            {'action': 'sync_zvol', 'zvol':zvol, 'target':target}, 'reply_to', self.nas_client.NODE_NAME, on_fail=ANY)
        self.assertTrue(self.check_zvol_busy(zvol))
        with sqlite3.connect(self.nas_client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('SELECT zvol, iscsi_target, remotehost FROM zvols WHERE zvol = ?',[zvol])
            self.assertSequenceEqual(cur.fetchone(), [zvol, target, 'compute-0-1'])




    @mock.patch('imgstorage.imgstoragenas.runCommand', return_value=
            ("tank/vm-hpcdev-pub03-1-vol@aaa\n"+
            "tank/vm-hpcdev-pub03-1-vol@bbb").splitlines())
    def test_find_last_snapshot(self, mock_run_command):
        zvol = 'vm-hpcdev-pub03-1-vol'
        self.assertEqual(self.nas_client.find_last_snapshot(zvol), 'bbb')


    def test_list_sync(self):
        with sqlite3.connect(self.nas_client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('INSERT INTO sync_queue VALUES(?,?,1,1,?)', ['vol3_busy', 'compute-0-3', 1408470839.3029799])
            con.commit()

        self.nas_client.list_sync({'action': 'list_sync'},
                    BasicProperties(reply_to='reply_to', correlation_id='message_id'))
        return_dict = [{'is_sending': 1, 'remotehost': u'compute-0-3', 'zvol': u'vol3_busy', 'time': 1408470839.3029799}]
        self.nas_client.queue_connector.publish_message.assert_called_with(
                    {'action': 'return_sync', 'status': 'success', 'body': return_dict}, routing_key='reply_to', exchange='')
        for d in return_dict:
                    print((
                        d['remotehost'],
                        "upload" if d['is_sending'] else "download",
                        d['zvol'],
                        str(datetime.timedelta(seconds=(int(time.time()-d['time']))))
                    ))


    def check_zvol_busy(self, zvol):
        with sqlite3.connect(self.nas_client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('SELECT count(*) from zvol_calls where zvol = ?',[zvol])
            num_rows = cur.fetchone()[0]
            return num_rows > 0


    def assertSequenceEqual(self, it1, it2):
        self.assertEqual(tuple(it1), tuple(it2))

tgtadm_response = """
Target 1: iqn.2001-04.com.nas-0-1-%s
    System information:
        Driver: iscsi
        State: ready
    I_T nexus information:
        I_T nexus: 1
            Initiator: iqn.1994-05.com.redhat:dd87ffb48f6e
            Connection: 0
                IP Address: 10.2.20.250
    LUN information:
        LUN: 0
            Type: controller
            SCSI ID: IET     00010000
            SCSI SN: beaf10
            Size: 0 MB, Block size: 1
            Online: Yes
            Removable media: No
            Prevent removal: No
            Readonly: No
            Backing store type: null
            Backing store path: None
            Backing store flags:
        LUN: 1
            Type: disk
            SCSI ID: IET     00010001
            SCSI SN: beaf11
            Size: 1074 MB, Block size: 512
            Online: Yes
            Removable media: No
            Prevent removal: No
            Readonly: No
            Backing store type: rdwr
            Backing store path: /dev/tank/%s
            Backing store flags:
    Account information:
    ACL information:
        10.2.20.250"""


tgt_setup_lun_response = """
Using transport: iscsi
Creating new target (name=iqn.2001-04.com.nas-0-1-%s, tid=1)
Adding a logical unit (/dev/tank/%s) to target, tid=1
Accepting connections only from 10.1.1.1"""

iscsiadm_session_response = """
iSCSI Transport Class version 2.0-870
version 6.2.0-873.10.el6
Target: iqn.2001-04.com.nas-0-1-compute-0-2-0-vol
    Current Portal: 10.2.20.254:3260,1
    Persistent Portal: 10.2.20.254:3260,1
        **********
        Interface:
        **********
        Iface Name: default
        Iface Transport: tcp
        Iface Initiatorname: iqn.1994-05.com.redhat:6453f1cc15cf
        Iface IPaddress: 10.2.20.252
        Iface HWaddress: <empty>
        Iface Netdev: <empty>
        SID: 49
        iSCSI Connection State: LOGGED IN
        iSCSI Session State: LOGGED_IN
        Internal iscsid Session State: NO CHANGE
        *********
        Timeouts:
        *********
        Recovery Timeout: 120
        Target Reset Timeout: 30
        LUN Reset Timeout: 30
        Abort Timeout: 15
        *****
        CHAP:
        *****
        username: <empty>
        password: ********
        username_in: <empty>
        password_in: ********
        ************************
        Negotiated iSCSI params:
        ************************
        HeaderDigest: None
        DataDigest: None
        MaxRecvDataSegmentLength: 262144
        MaxXmitDataSegmentLength: 8192
        FirstBurstLength: 65536
        MaxBurstLength: 262144
        ImmediateData: Yes
        InitialR2T: Yes
        MaxOutstandingR2T: 1
        ************************
        Attached SCSI devices:
        ************************
        Host Number: 54 State: running
        scsi54 Channel 00 Id 0 Lun: 0
        scsi54 Channel 00 Id 0 Lun: 1
            Attached scsi disk sdb      State: running
Target: %s
    Current Portal: 10.2.20.247:3260,1
    Persistent Portal: 10.2.20.247:3260,1
        **********
        Interface:
        **********
        Iface Name: default
        Iface Transport: tcp
        Iface Initiatorname: iqn.1994-05.com.redhat:6453f1cc15cf
        Iface IPaddress: <empty>
        Iface HWaddress: <empty>
        Iface Netdev: <empty>
        SID: 69
        iSCSI Connection State: TRANSPORT WAIT
        iSCSI Session State: FREE
        Internal iscsid Session State: REOPEN
        *********
        Timeouts:
        *********
        Recovery Timeout: 120
        Target Reset Timeout: 30
        LUN Reset Timeout: 30
        Abort Timeout: 15
        *****
        CHAP:
        *****
        username: <empty>
        password: ********
        username_in: <empty>
        password_in: ********
        ************************
        Negotiated iSCSI params:
        ************************
        HeaderDigest: None
        DataDigest: None
        MaxRecvDataSegmentLength: 262144
        MaxXmitDataSegmentLength: 8192
        FirstBurstLength: 65536
        MaxBurstLength: 262144
        ImmediateData: Yes
        InitialR2T: Yes
        MaxOutstandingR2T: 1
        ************************
        Attached SCSI devices:
        ************************
        Host Number: 74 State: running
        scsi74 Channel 00 Id 0 Lun: 0
        scsi74 Channel 00 Id 0 Lun: 1
            Attached scsi disk %s      State: transport-offline"""

