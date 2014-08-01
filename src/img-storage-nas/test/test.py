#!/opt/rocks/bin/python

import sys, os
lib_path = os.path.abspath('../img-storage')
sys.path.append(lib_path)

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
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol2', 'iqn.2001-04.com.nas-0-1-vol2', 'nas-0-1'))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol3_busy', 'iqn.2001-04.com.nas-0-1-vol3_busy', 'nas-0-1'))
            cur.execute('INSERT INTO zvol_calls VALUES (?,?,?)',('vol3_busy', 'reply_to', time.time()))
            con.commit()

    def tearDown(self):
        os.remove(self.client.SQLITE_DB)



    """ Testing mapping of newly created zvol """
    @mock.patch('imgstorage.imgstoragenas.runCommand')
    @mock.patch('socket.gethostbyname', return_value='10.1.1.1')
    def test_set_zvol_createnew_success(self, mockGetHostCommand, mockRunCommand):
        zvol = 'vol3'
        def my_side_effect(*args, **kwargs):
            if args[0][0] == 'zfs':                return StringIO("")
            elif args[0][0] == 'tgt-setup-lun':    return StringIO(tgt_setup_lun_response%(zvol, zvol))

        mockRunCommand.side_effect = my_side_effect
        self.client.ib_net = 'ibnet'
        self.client.set_zvol(
            {'action': 'set_zvol', 'zvol': zvol, 'hosting': 'compute-0-1', 'size': '10gb'},
            BasicProperties(reply_to='reply_to'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'set_zvol', 'nas': 'hpcdev-pub02.ibnet', 'target': 'iqn.2001-04.com.nas-0-1-%s'%(zvol)}, 'compute-0-1', 'hpcdev-pub02', on_fail=ANY)


    """ Testing mapping of zvol created before """
    @mock.patch('imgstorage.imgstoragenas.runCommand')
    @mock.patch('socket.gethostbyname', return_value='10.1.1.1')
    def test_set_zvol_usecreated_success(self, mockGetHostCommand, mockRunCommand ):
        zvol = 'vol1'
        def my_side_effect(*args, **kwargs):
            if args[0][0] == 'zfs':                return StringIO("")
            elif args[0][0] == 'tgt-setup-lun':    return StringIO(tgt_setup_lun_response%(zvol, zvol))
        mockRunCommand.side_effect = my_side_effect

        self.client.ib_net = 'ibnet'
        self.client.set_zvol(
            {'action': 'set_zvol', 'zvol': zvol, 'hosting': 'compute-0-1', 'size': '10gb'},
            BasicProperties(reply_to='reply_to'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'set_zvol', 'nas': 'hpcdev-pub02.ibnet', 'target': 'iqn.2001-04.com.nas-0-1-%s'%(zvol)}, 'compute-0-1', 'hpcdev-pub02', on_fail=ANY)



    """ Testing mapping of busy zvol """
    @mock.patch('imgstorage.imgstoragenas.runCommand')
    @mock.patch('socket.gethostbyname', return_value='10.1.1.1')
    def test_set_zvol_busy(self, mockGetHostCommand, mockRunCommand ):
        zvol = 'vol3_busy'
        def my_side_effect(*args, **kwargs): # just in case... not used in normal condition
            if args[0][0] == 'zfs':                return StringIO("")
            elif args[0][0] == 'tgt-setup-lun':    return StringIO(tgt_setup_lun_response%(zvol, zvol))
        mockRunCommand.side_effect = my_side_effect

        self.client.ib_net = 'ibnet'
        self.client.set_zvol(
            {'action': 'set_zvol', 'zvol': zvol, 'hosting': 'compute-0-1', 'size': '10gb'},
            BasicProperties(reply_to='reply_to'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_attached', 'status': 'error', 'error': 'ZVol %s is busy'%zvol}, routing_key='reply_to', exchange='')



 
    def test_fail_action(self):
        self.client.failAction('routing_key', 'action', 'error_message')
        self.client.queue_connector.publish_message.assert_called_with(
                {'action': 'action', 'status': 'error', 'error': 'error_message'}, 
                routing_key='routing_key', 
                exchange='')

    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_teardown_success(self, mockRunCommand):
        zvol = 'vol2'
        mockRunCommand.return_value = StringIO(tgtadm_response%(zvol, zvol))

        self.client.tear_down(
            {'action': 'tear_down', 'zvol': zvol},
            BasicProperties(reply_to='reply_to'))

        mockRunCommand.assert_called_with(['tgtadm', '--op', 'show', '--mode', 'target'])

        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'tear_down', 'target': u'iqn.2001-04.com.nas-0-1-%s'%zvol}, u'nas-0-1', 'hpcdev-pub02', on_fail=ANY)

    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_teardown_busy(self, mockRunCommand):
        zvol = 'vol3_busy'
        mockRunCommand.return_value = StringIO(tgtadm_response%(zvol, zvol))

        self.client.tear_down(
            {'action': 'tear_down', 'zvol': zvol},
            BasicProperties(reply_to='reply_to'))

        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_detached', 'status': 'error', 'error': 'ZVol %s is busy'%zvol}, routing_key='reply_to', exchange='')

    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_teardown_detached_volume(self, mockRunCommand):
        zvol = 'vol1'
        mockRunCommand.return_value = StringIO(tgtadm_response%(zvol, zvol))

        self.client.tear_down(
            {'action': 'tear_down', 'zvol': zvol},
            BasicProperties(reply_to='reply_to'))

        self.client.queue_connector.publish_message.assert_called_with(
                {'action': 'zvol_detached', 'status': 'error', 'error': 'ZVol %s is not attached'%zvol}, 
                routing_key='reply_to', 
                exchange='')


    @mock.patch('imgstorage.imgstoragenas.runCommand', return_value='')
    def test_del_zvol_success(self, mockRunCommand):
        zvol = 'vol1'
        self.client.del_zvol(
            {'action': 'del_zvol', 'zvol': zvol},
            BasicProperties(reply_to='reply_to'))

        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_deleted', 'status': 'success'}, routing_key='reply_to', exchange='')


    @mock.patch('imgstorage.imgstoragenas.runCommand', return_value='')
    def test_del_zvol_not_found(self, mockRunCommand):
        zvol = 'wrong_vol'
        self.client.del_zvol(
            {'action': 'del_zvol', 'zvol': zvol},
            BasicProperties(reply_to='reply_to'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_deleted', 'status': 'error', 'error': 'ZVol wrong_vol not found in database'}, 
            routing_key='reply_to', exchange='')



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
