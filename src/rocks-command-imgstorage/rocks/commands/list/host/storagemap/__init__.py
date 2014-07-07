#!/opt/rocks/bin/python
#
#

import sys
import string
import rocks.commands
import pika

import json
import uuid
import logging
logging.basicConfig()

import rocks.db.helper
db = rocks.db.helper.DatabaseHelper()
db.connect()
RABBITMQ_SERVER = db.getHostAttr(db.getHostname('localhost'), 'Kickstart_PrivateHostname')
db.close()

RABBITMQ_URL = 'amqp://guest:guest@%s:5672/%%2F?connection_attempts=3&heartbeat_interval=3600'%RABBITMQ_SERVER


class Command(rocks.commands.HostArgumentProcessor, rocks.commands.list.command):
        """
        List the current active mapping on a NAS (or virtual machine images 
        repository).
        
        <arg type='string' name='nas' optional='0'>
        The NAS name which we want to interrogate
        </arg>

        <example cmd='list host storagemap nas-0-0'>
        It will display the list of mappings on nas-0-0
        </example>
        """

        def callListHostStoragemap(self, nas):
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))

            channel = connection.channel()

            # Declare the queue
            method_frame = channel.queue_declare(exclusive=True, auto_delete=True)
            zvol_manage_queue = method_frame.method.queue
            channel.confirm_delivery()

            message = {'action': 'list_zvols'}

            def on_message(channel, method_frame, header_frame, body):
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                channel.stop_consuming()
                channel.close()
                message = json.loads(body)
                if(message['status'] == 'success'):
                    self.beginOutput()
                    arr = message['body']
                    for d in message['body']:
                        self.addOutput(nas, d.values())
                    headers=['nas','device', 'zvol', 'host']
                    self.endOutput(headers)
                else:
                    error_msg = ''
                    if 'error_description' in message:
                        error_msg = message['error_description']
                    self.abort('%s %s'%(message['error'], error_msg))

            # Send a message
            if channel.basic_publish(exchange='rocks.vm-manage',
                                 routing_key=nas,
                                 mandatory=True,
                                 body=json.dumps(message, ensure_ascii=True),
                                 properties=pika.BasicProperties(content_type='application/json',
                                                                 delivery_mode=1,
                                                                 correlation_id = str(uuid.uuid4()),
                                                                 reply_to = zvol_manage_queue
                                                                )
                                ):
                channel.basic_consume(on_message, zvol_manage_queue)
                channel.start_consuming()
            else:
                self.abort('Message could not be delivered: ')
        #def list(self, nas):
                # Im not too sure what is the best way to implement this
                # what is the right set of api

                # return list of tuple in the form of (zvolname, mappedhost, devicename)
                #return [("zpool/vm-sdsc125-2","compute-0-0","/dev/sdc"), 
                #        ("zpool/vm-sdsc125-3","compute-0-1","/dev/sdc")]


        def run(self, params, args):
                (args, nas) = self.fillPositionalArgs(('nas'))

                if not nas:
                        self.abort("you must enter the nas name")
                # debugging output
                ret = self.callListHostStoragemap(nas)




RollName = "kvm"
