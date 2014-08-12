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

class TestNasFunctions(unittest.TestCase):

    def mock_rabbitcli(self, exchange, exchange_type, process_message=None):
        class MockRabbitMQCommonClient(RabbitMQCommonClient):
            def publish_message(self, message, routing_key=None, reply_to=None, exchange=None, correlation_id=None, on_fail=None):
                return
        return MockRabbitMQCommonClient

    @mock.patch('imgstorage.imgstoragenas.RabbitMQCommonClient')
    def setUp(self, mock_rabbit):
        self.client = NasDaemon()
        mock_rabbit.publish_message = MagicMock()
        self.client.process_message = MagicMock()

        self.client.SQLITE_DB = '/tmp/test_db_%s'%uuid.uuid4()
        self.client.run()

        with sqlite3.connect(self.client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol1', None, None))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol2', 'iqn.2001-04.com.nas-0-1-vol2', 'compute-0-3'))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol3_busy', 'iqn.2001-04.com.nas-0-1-vol3_busy', 'compute-0-3'))
            cur.execute('INSERT INTO zvol_calls VALUES (?,?,?)',('vol3_busy', 'reply_to', time.time()))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol4_busy', 'iqn.2001-04.com.nas-0-1-vol4_busy', 'compute-0-3'))
            cur.execute('INSERT INTO zvol_calls VALUES (?,?,?)',('vol4_busy', 'reply_to', time.time()))
            con.commit()

    def tearDown(self):
        os.remove(self.client.SQLITE_DB)



    """ Testing mapping of newly created zvol """
    @mock.patch('imgstorage.imgstoragenas.runCommand')
    @mock.patch('socket.gethostbyname', return_value='10.1.1.1')
    def test_map_zvol_createnew_success(self, mockGetHostCommand, mockRunCommand):
        zvol = 'vol3'
        def my_side_effect(*args, **kwargs):
            if args[0][0] == 'zfs':                return StringIO("")
            elif args[0][0] == 'tgt-setup-lun':    return StringIO(tgt_setup_lun_response%(zvol, zvol))

        mockRunCommand.side_effect = my_side_effect
        self.client.ib_net = 'ibnet'
        self.client.map_zvol(
            {'action': 'map_zvol', 'zvol': zvol, 'remotehost': 'compute-0-1', 'size': '10'},
            BasicProperties(reply_to='reply_to'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'map_zvol', 'zvol': zvol, 'nas': '%s.ibnet'%self.client.NODE_NAME, 'target': 'iqn.2001-04.com.nas-0-1-%s'%(zvol), 'size':'10'}, 'compute-0-1', self.client.NODE_NAME, on_fail=ANY)
        self.assertTrue(self.check_zvol_busy(zvol))


    """ Testing mapping of zvol created before """
    @mock.patch('imgstorage.imgstoragenas.runCommand')
    @mock.patch('socket.gethostbyname', return_value='10.1.1.1')
    def test_map_zvol_usecreated_success(self, mockGetHostCommand, mockRunCommand ):
        zvol = 'vol1'
        def my_side_effect(*args, **kwargs):
            if args[0][0] == 'zfs':                return StringIO("")
            elif args[0][0] == 'tgt-setup-lun':    return StringIO(tgt_setup_lun_response%(zvol, zvol))
        mockRunCommand.side_effect = my_side_effect

        self.client.ib_net = 'ibnet'
        self.client.map_zvol(
            {'action': 'map_zvol', 'zvol': zvol, 'remotehost': 'compute-0-1', 'size': '10'},
            BasicProperties(reply_to='reply_to'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'map_zvol', 'zvol': zvol, 'nas': '%s.ibnet'%self.client.NODE_NAME, 'target': 'iqn.2001-04.com.nas-0-1-%s'%(zvol), 'size':'10'}, 'compute-0-1', self.client.NODE_NAME, on_fail=ANY)
        self.assertTrue(self.check_zvol_busy(zvol))


    """ Testing mapping of busy zvol """
    @mock.patch('imgstorage.imgstoragenas.runCommand')
    @mock.patch('socket.gethostbyname', return_value='10.1.1.1')
    def test_map_zvol_busy(self, mockGetHostCommand, mockRunCommand ):
        zvol = 'vol3_busy'
        def my_side_effect(*args, **kwargs): # just in case... not used in normal condition
            if args[0][0] == 'zfs':                return StringIO("")
            elif args[0][0] == 'tgt-setup-lun':    return StringIO(tgt_setup_lun_response%(zvol, zvol))
        mockRunCommand.side_effect = my_side_effect

        self.client.ib_net = 'ibnet'
        self.client.map_zvol(
            {'action': 'map_zvol', 'zvol': zvol, 'remotehost': 'compute-0-1', 'size': '10'},
            BasicProperties(reply_to='reply_to'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_mapped', 'status': 'error', 'error': 'ZVol %s is busy'%zvol}, routing_key='reply_to', exchange='')
        self.assertTrue(self.check_zvol_busy(zvol))


    def test_fail_action(self):
        self.client.failAction('routing_key', 'action', 'error_message')
        self.client.queue_connector.publish_message.assert_called_with(
                {'action': 'action', 'status': 'error', 'error': 'error_message'},
                routing_key='routing_key',
                exchange='')

    @mock.patch('imgstorage.imgstoragenas.runCommand')
    @mock.patch('imgstorage.imgstoragenas.rocks.db.helper.DatabaseHelper.getHostAttr', return_value=False)
    def test_teardown_success(self, mockHostSyncAttr, mockRunCommand):
        zvol = 'vol2'
        mockRunCommand.return_value = StringIO(tgtadm_response%(zvol, zvol))

        self.client.unmap_zvol(
            {'action': 'unmap_zvol', 'zvol': zvol},
            BasicProperties(reply_to='reply_to'))

        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'unmap_zvol', 'target': u'iqn.2001-04.com.nas-0-1-%s'%zvol, 'zvol':zvol}, u'compute-0-3', self.client.NODE_NAME, on_fail=ANY)
        self.assertTrue(self.check_zvol_busy(zvol))


    @mock.patch('imgstorage.imgstoragenas.runCommand')
    @mock.patch('imgstorage.imgstoragenas.rocks.db.helper.DatabaseHelper.getHostAttr', return_value=False)
    def test_teardown_busy(self, mockHostSyncAttr, mockRunCommand):
        zvol = 'vol3_busy'
        mockRunCommand.return_value = StringIO(tgtadm_response%(zvol, zvol))

        self.client.unmap_zvol(
            {'action': 'unmap_zvol', 'zvol': zvol},
            BasicProperties(reply_to='reply_to'))

        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_unmapped', 'status': 'error', 'error': 'ZVol %s is busy'%zvol}, routing_key='reply_to', exchange='')
        self.assertTrue(self.check_zvol_busy(zvol))


    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_teardown_unmapped_volume(self, mockRunCommand):
        zvol = 'vol1'
        mockRunCommand.return_value = StringIO(tgtadm_response%(zvol, zvol))

        self.client.unmap_zvol(
            {'action': 'unmap_zvol', 'zvol': zvol},
            BasicProperties(reply_to='reply_to'))

        self.client.queue_connector.publish_message.assert_called_with(
                {'action': 'zvol_unmapped', 'status': 'error', 'error': 'ZVol %s is not mapped'%zvol},
                routing_key='reply_to',
                exchange='')
        self.assertFalse(self.check_zvol_busy(zvol))



    @mock.patch('imgstorage.imgstoragenas.runCommand', return_value='')
    def test_del_zvol_success(self, mockRunCommand):
        zvol = 'vol1'
        self.client.del_zvol(
            {'action': 'del_zvol', 'zvol': zvol},
            BasicProperties(reply_to='reply_to'))

        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_deleted', 'status': 'success'}, routing_key='reply_to', exchange='')
        mockRunCommand.assert_called_with(['zfs', 'destroy', 'tank/%s'%(zvol), '-r'])
        self.assertFalse(self.check_zvol_busy(zvol))


    @mock.patch('imgstorage.imgstoragenas.runCommand', return_value='')
    def test_del_zvol_not_found(self, mockRunCommand):
        zvol = 'wrong_vol'
        self.client.del_zvol(
            {'action': 'del_zvol', 'zvol': zvol},
            BasicProperties(reply_to='reply_to'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_deleted', 'status': 'error', 'error': 'ZVol %s not found in database'%zvol},
            routing_key='reply_to', exchange='')
        self.assertFalse(self.check_zvol_busy(zvol))


    @mock.patch('imgstorage.imgstoragenas.runCommand', return_value='')
    def test_del_zvol_mapped(self, mockRunCommand):
        zvol = 'vol2'
        self.client.del_zvol(
            {'action': 'del_zvol', 'zvol': zvol},
            BasicProperties(reply_to='reply_to'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_deleted', 'status': 'error', 'error': 'Error deleting zvol %s: is mapped'%zvol},
            routing_key='reply_to', exchange='')
        self.assertFalse(self.check_zvol_busy(zvol))

    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_find_iscsi_target_num_not_found(self, mockRunCommand):
        zvol = 'vol1'
        target = 'not_found_iqn.2001-04.com.nas-0-1-%s'%zvol
        mockRunCommand.return_value = StringIO(tgtadm_response%(zvol, zvol))
        self.assertEqual(self.client.find_iscsi_target_num(target), None)


    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_find_iscsi_target_num_success(self, mockRunCommand):
        zvol = 'vol1'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        mockRunCommand.return_value = StringIO(tgtadm_response%(zvol, zvol))
        self.assertEqual(self.client.find_iscsi_target_num(target), '1')


    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_zvol_unmapped_success(self, mockRunCommand):
        zvol = 'vol3_busy'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        self.client.zvol_unmapped(
            {'action': 'zvol_unmapped', 'target':target, 'zvol':zvol, 'status':'success'},
            BasicProperties(reply_to='reply_to', correlation_id='message_id'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_unmapped', 'status': 'success'}, routing_key=u'reply_to', exchange='')
        self.assertFalse(self.check_zvol_busy(zvol))


    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_zvol_unmapped_got_error(self, mockRunCommand):
        zvol = 'vol3_busy'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        self.client.zvol_unmapped(
            {'action': 'zvol_unmapped', 'target':target, 'zvol':zvol, 'status':'error', 'error':'Some error'},
            BasicProperties(reply_to='reply_to', correlation_id='message_id'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_unmapped', 'status': 'error', 'error': 'Error detaching iSCSI target from compute node: Some error'}, routing_key=u'reply_to', exchange='')
        self.assertFalse(self.check_zvol_busy(zvol))


    def test_zvol_mapped_success(self):
        zvol = 'vol4_busy'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        self.client.zvol_mapped(
            {'action': 'zvol_mapped', 'target':target, 'bdev': 'sdc', 'status':'success'},
            BasicProperties(reply_to='reply_to', correlation_id='message_id'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_mapped', 'status': 'success', 'bdev': 'sdc'}, routing_key=u'reply_to', exchange='')
        self.assertFalse(self.check_zvol_busy(zvol))

    def test_zvol_mapped_got_error(self):
        zvol = 'vol4_busy'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        self.client.zvol_mapped(
            {'action': 'zvol_mapped', 'target':target, 'status':'error', 'error':'Some error'},
            BasicProperties(reply_to='reply_to', correlation_id='message_id'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_mapped', 'status': 'error', 'error': 'Error attaching iSCSI target to compute node: Some error'}, routing_key=u'reply_to', exchange='')
        self.assertFalse(self.check_zvol_busy(zvol)) # TODO IS THIS RIGHT?

    def check_zvol_busy(self, zvol):
        with sqlite3.connect(self.client.SQLITE_DB) as con:
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
