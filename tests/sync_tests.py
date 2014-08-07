#!/opt/rocks/bin/python

import sys, os
lib_path = os.path.abspath('src/img-storage')
sys.path.insert(1, lib_path)

import unittest
from mock import MagicMock, ANY
import mock
from imgstorage.imgstoragesync import SyncDaemon
from imgstorage.imgstoragevm import VmDaemon
from imgstorage.rabbitmqclient import RabbitMQCommonClient

import uuid
import time

from pysqlite2 import dbapi2 as sqlite3

from pika.spec import BasicProperties
from StringIO import StringIO

class TestNasFunctions(unittest.TestCase):

    def mock_rabbitcli(self, exchange, exchange_type, process_message=None):
        class MockRabbitMQCommonClient(RabbitMQCommonClient):
            def publish_message(self, message, routing_key=None, reply_to=None, exchange=None, correlation_id=None, on_fail=None):
                return
        return MockRabbitMQCommonClient

    @mock.patch('imgstorage.imgstoragenas.RabbitMQCommonClient')
    def setUp(self, mock_rabbit):
        self.nas_client = SyncDaemon()
        self.vm_client = VmDaemon()
        mock_rabbit.publish_message = MagicMock()
        self.nas_client.process_message = MagicMock()
        self.vm_client.process_message = MagicMock()

        self.nas_client.SQLITE_DB = '/tmp/test_db_%s'%uuid.uuid4()
        self.nas_client.run()
        self.vm_client.run()

        with sqlite3.connect(self.nas_client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol1', None, None))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol2', 'iqn.2001-04.com.nas-0-1-vol2', 'nas-0-1'))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol3_busy', 'iqn.2001-04.com.nas-0-1-vol3_busy', 'nas-0-1'))
            cur.execute('INSERT INTO zvol_calls VALUES (?,?,?)',('vol3_busy', 'reply_to', time.time()))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol4_busy', 'iqn.2001-04.com.nas-0-1-vol4_busy', 'nas-0-1'))
            cur.execute('INSERT INTO zvol_calls VALUES (?,?,?)',('vol4_busy', 'reply_to', time.time()))
            con.commit()

    def tearDown(self):
        os.remove(self.nas_client.SQLITE_DB)


    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_zvol_mapped_success(self, mockRunCommand):
        zvol = 'vol4_busy'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol

        mockRunCommand.assert_any_call(['zfs', 'snap', u'tank/%s@initial_snapshot'%zvol])
        mockRunCommand.assert_any_call(['zfs', 'send', u'tank/%s@initial_snapshot'%zvol], ['ssh', 'compute-0-3', 'zfs', 'receive', '-F', 'tank/%s'%zvol])

        self.nas_client.zvol_mapped(
            {'action': 'zvol_mapped', 'target':target, 'bdev': '/dev/mapper/%s-snap'%zvol, 'status':'success'},
            BasicProperties(reply_to='reply_to', correlation_id='message_id'))
        self.nas_client.queue_connector.publish_message.assert_any_call(
            {'action': 'sync_zvol', 'zvol':zvol, 'target':target}, 'reply_to', self.nas_client.NODE_NAME, on_fail=ANY)
        self.assertTrue(self.check_zvol_busy(zvol))



    @mock.patch('imgstorage.imgstoragevm.is_sync_enabled', return_value=True)
    def test_zvol_synced_success(self, mock_sync_enabled):
        zvol = 'vol4_busy'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        self.nas_client.zvol_synced(
            {'action': 'zvol_synced', 'status':'success', 'zvol':zvol},
            BasicProperties(reply_to='reply_to', correlation_id='message_id'))
        self.assertFalse(self.check_zvol_busy(zvol))

    # def test_zvol_mapped_got_error(self):
    #     zvol = 'vol4_busy'
    #     target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
    #     self.client.zvol_mapped(
    #         {'action': 'zvol_mapped', 'target':target, 'status':'error', 'error':'Some error'},
    #         BasicProperties(reply_to='reply_to', correlation_id='message_id'))
    #     self.client.queue_connector.publish_message.assert_called_with(
    #         {'action': 'zvol_mapped', 'status': 'error', 'error': 'Error attaching iSCSI target to compute node: Some error'}, routing_key=u'reply_to', exchange='')
    #     self.assertTrue(self.check_zvol_busy(zvol)) # TODO IS THIS RIGHT?

    def check_zvol_busy(self, zvol):
        with sqlite3.connect(self.nas_client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('SELECT count(*) from zvol_calls where zvol = ?',[zvol])
            num_rows = cur.fetchone()[0]
            return num_rows > 0


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
