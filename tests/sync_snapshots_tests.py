#!/opt/rocks/bin/python

import sys, os
lib_path = os.path.abspath('src/img-storage')
sys.path.insert(1, lib_path)

import unittest
from mock import MagicMock, ANY
import mock
from imgstorage.imgstoragenas import NasDaemon
from imgstorage.imgstoragevm import VmDaemon
from imgstorage.rabbitmqclient import RabbitMQCommonClient

import uuid
import time

from pysqlite2 import dbapi2 as sqlite3

from pika.spec import BasicProperties
from StringIO import StringIO

import datetime

class TestSyncSnapshotsFunctions(unittest.TestCase):

    def mock_rabbitcli(self, exchange, exchange_type, process_message=None):
        class MockRabbitMQCommonClient(RabbitMQCommonClient):
            def publish_message(self, message, routing_key=None, reply_to=None, exchange=None, correlation_id=None, on_fail=None):
                return
        return MockRabbitMQCommonClient

    @mock.patch('imgstorage.imgstoragenas.RabbitMQCommonClient')
    def setUp(self, mock_rabbit_nas):
        self.nas_client = NasDaemon()
        mock_rabbit_nas.publish_message = MagicMock()
        self.nas_client.process_message = MagicMock()

        self.nas_client.SQLITE_DB = '/tmp/test_db_%s'%uuid.uuid4()
        self.nas_client.run()

        with sqlite3.connect(self.nas_client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS zvol_calls(zvol TEXT PRIMARY KEY NOT NULL, reply_to TEXT NOT NULL, time INT NOT NULL)')
            cur.execute('CREATE TABLE IF NOT EXISTS zvols(zvol TEXT PRIMARY KEY NOT NULL, iscsi_target TEXT UNIQUE, remotehost TEXT)')
            cur.execute('CREATE TABLE IF NOT EXISTS sync_queue(zvol TEXT PRIMARY KEY NOT NULL, remotehost TEXT, is_sending BOOLEAN, is_delete_remote BOOLEAN, time INT)')
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol1', None, None))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol2', None, 'compute-0-1'))
            cur.execute('INSERT INTO zvols VALUES (?,?,?) ',('vol3', 'target_nas-0-1', 'compute-0-1'))
            con.commit()

    def tearDown(self):
        os.remove(self.nas_client.SQLITE_DB)


    @mock.patch('imgstorage.imgstoragenas.runCommand')
    def test_schedule_download(self, mock_run_command):
        with sqlite3.connect(self.nas_client.SQLITE_DB) as con:
            cur = con.cursor()
            cur.execute('INSERT INTO sync_queue VALUES(?,?,0,1,?)', ['vol3_busy', 'compute-0-3', time.time()])
            con.commit()

        self.nas_client.schedule_next_sync()
        print mock_run_command.mock_calls
