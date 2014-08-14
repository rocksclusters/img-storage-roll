#!/opt/rocks/bin/python

import sys, os
lib_path = os.path.abspath('src/img-storage')
sys.path.insert(1, lib_path)

import unittest
from mock import MagicMock, ANY
import mock
from imgstorage.imgstoragevm import VmDaemon
from imgstorage.rabbitmqclient import RabbitMQCommonClient
from imgstorage import ActionError

import uuid
import time

from pysqlite2 import dbapi2 as sqlite3

from pika.spec import BasicProperties
from StringIO import StringIO

class TestVmFunctions(unittest.TestCase):

    def mock_rabbitcli(self, exchange, exchange_type, process_message=None):
        class MockRabbitMQCommonClient(RabbitMQCommonClient):
            def publish_message(self, message, routing_key=None, reply_to=None, exchange=None, correlation_id=None, on_fail=None):
                return
        return MockRabbitMQCommonClient

    @mock.patch('imgstorage.imgstoragevm.RabbitMQCommonClient')
    def setUp(self, mock_rabbit):
        self.client = VmDaemon()
        mock_rabbit.publish_message = MagicMock()
        self.client.process_message = MagicMock()

        self.client.run()

        self.client.SQLITE_DB = '/tmp/test_db_%s'%uuid.uuid4()
        self.client.run()

        with sqlite3.connect(self.client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('INSERT INTO zvol_calls VALUES (?,?,?,?,?,?,?)',('vol1', 'iqn.2001-04.com.nas-0-1-vol1', 12345, 'reply_to', 'corr_id', 0, 1))
            con.commit()


    """ Testing mapping of zvol """
    @mock.patch('imgstorage.imgstoragevm.runCommand')
    @mock.patch('imgstorage.imgstoragevm.VmDaemon.is_sync_enabled', return_value=False)
    def test_map_zvol_createnew_success(self, mockSyncEnabled, mockRunCommand):
        zvol = 'vol2'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        bdev = 'sdc'

        mockRunCommand.side_effect = self.create_iscsiadm_side_effect(target, bdev)
        self.client.map_zvol(
            {'action': 'map_zvol', 'target':target, 'nas': 'nas-0-1', 'size':'35', 'zvol':zvol},
            BasicProperties(reply_to='reply_to', message_id='message_id'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_mapped', 'status': 'success', 'bdev': '/dev/%s'%bdev, 'target': target}, 'reply_to', reply_to=self.client.NODE_NAME, correlation_id='message_id')
        mockRunCommand.assert_any_call(['iscsiadm', '-m', 'discovery', '-t', 'sendtargets', '-p', 'nas-0-1'])
        mockRunCommand.assert_any_call(['iscsiadm', '-m', 'node', '-T', target, '-p', 'nas-0-1', '-l'])
        mockRunCommand.assert_any_call(['iscsiadm', '-m', 'session', '-P3'])
        assert 3 == mockRunCommand.call_count

    """ Testing mapping of zvol for missing block device """
    @mock.patch('imgstorage.imgstoragevm.runCommand')
    @mock.patch('imgstorage.imgstoragevm.VmDaemon.is_sync_enabled', return_value=False)
    def test_map_zvol_createnew_missing_blkdev_error(self, mockSyncEnabled, mockRunCommand):
        zvol = 'vol2'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        bdev = 'sdc'

        mockRunCommand.side_effect = self.create_iscsiadm_side_effect(target+"_missing_target", bdev)
        self.client.map_zvol(
            {'action': 'map_zvol', 'target':target, 'nas': 'nas-0-1', 'size':'35', 'zvol':zvol},
            BasicProperties(reply_to='reply_to', message_id='message_id'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_mapped', 'status': 'error', 'target': target,
            'error': 'Not found %s in targets'%target},
            'reply_to', reply_to=self.client.NODE_NAME, correlation_id='message_id')

    """ Testing unmapping of zvol """
    @mock.patch('imgstorage.imgstoragevm.runCommand')
    def test_unmap_zvol_success(self, mockRunCommand):
        zvol = 'vol2'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        bdev = 'sdc'

        mockRunCommand.side_effect = self.create_iscsiadm_side_effect(target, bdev)
        self.client.unmap_zvol(
            {'action': 'unmap_zvol', 'target':target, 'zvol':zvol},
            BasicProperties(reply_to='reply_to', message_id='message_id'))
        print self.client.queue_connector.publish_message.mock_calls
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_unmapped', 'status': 'success', 'target': target, 'zvol':zvol}, 'reply_to', reply_to=self.client.NODE_NAME, correlation_id='message_id')
        mockRunCommand.assert_any_call(['iscsiadm', '-m', 'node', '-T', target, '-u'])
        mockRunCommand.assert_any_call(['iscsiadm', '-m', 'session', '-P3'])

    """ Testing unmapping of zvol when not found - still returns success """
    @mock.patch('imgstorage.imgstoragevm.runCommand')
    def test_unmap_zvol_not_found(self, mockRunCommand):
        zvol = 'vol2'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        bdev = 'sdc'

        mockRunCommand.side_effect = self.create_iscsiadm_side_effect(target, bdev)
        self.client.unmap_zvol(
            {'action': 'unmap_zvol', 'target':target+"not_found", 'zvol':zvol},
            BasicProperties(reply_to='reply_to', message_id='message_id'))
        print self.client.queue_connector.publish_message.mock_calls

        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_unmapped', 'status': 'success', 'target': target+"not_found", 'zvol':zvol}, 'reply_to', reply_to=self.client.NODE_NAME, correlation_id='message_id')


    """ Testing unmapping of zvol with error from system call """
    @mock.patch('imgstorage.imgstoragevm.runCommand')
    def test_map_zvol_unmap_error(self, mockRunCommand):
        zvol = 'vol2'
        target = 'iqn.2001-04.com.nas-0-1-%s'%zvol
        bdev = 'sdc'
        def my_side_effect(*args, **kwargs):
            if args[0][:3] == ['iscsiadm', '-m', 'session']:    return StringIO(iscsiadm_session_response%(target, bdev))
            elif args[0][:3] == ['iscsiadm', '-m', 'discovery']:    return StringIO(iscsiadm_discovery_response%target) # find remote targets
            elif args[0][:3] == ['iscsiadm', '-m', 'node']:
                raise ActionError('Some error happened')

        mockRunCommand.side_effect = my_side_effect
        self.client.unmap_zvol(
            {'action': 'unmap_zvol', 'target':target, 'zvol':zvol},
            BasicProperties(reply_to='reply_to', message_id='message_id'))
        self.client.queue_connector.publish_message.assert_called_with(
            {'action': 'zvol_unmapped', 'status': 'error', 'target': target, 'zvol':zvol, 'error': 'Some error happened'},
            'reply_to', reply_to=self.client.NODE_NAME,  correlation_id='message_id')
        mockRunCommand.assert_called_with(['iscsiadm', '-m', 'node', '-T', target, '-u'])

    def create_iscsiadm_side_effect(self, target, bdev):
        def iscsiadm_side_effect(*args, **kwargs):
            if args[0][:3] == ['iscsiadm', '-m', 'session']:        return (iscsiadm_session_response%(target, bdev)).splitlines() # list local devices
            elif args[0][:3] == ['iscsiadm', '-m', 'discovery']:    return (iscsiadm_discovery_response%target).splitlines() # find remote targets
            elif args[0][:3] == ['iscsiadm', '-m', 'node']:         return '\n'.splitlines() # connect to iscsi target
            elif args[0][0] == 'blockdev':                          return '12345'.splitlines()
        return iscsiadm_side_effect


iscsiadm_discovery_response = """
10.2.20.247:3260,1 iqn.2001-04.com.nas-0-1-vm-hpcdev-pub03-1-vol
10.2.20.247:3260,2 %s"""

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

