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

class Command(rocks.commands.HostArgumentProcessor, rocks.commands.add.command):
        """
        Map a virtual machine image from the NAS (or virtual machine images 
        repository) to the hosting environment.
        
        <arg type='string' name='nas' optional='0'>
        The NAS name which will host the storage image
        </arg>

        <arg type='string' name='volume' optional='0'>
        The volume name which will be mapped on the hosting environment
        </arg>

        <arg type='string' name='hosting' optional='0'>
        The machine name that will mount the storage volume
        </arg>

        <arg type='string' name='size' optional='0'>
        The size of the volume in Gigabyte.
        If the disk is already present on the NAS the size will be ignored.
        </arg>

        <example cmd='add host storagemap nas-0-0 zpool/vm-sdsc125-2 compute-0-0 35'>
        If it does not exist create zpool/vm-sdsc125-2 on nas and map it to 
        compute-0-0-0.
        </example>
        """

        block_dev = ''

        def callAddHostStoragemap(self, nas, volume, hosting, size):
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))

            channel = connection.channel()

            # Declare the queue
            method_frame = channel.queue_declare(exclusive=True, auto_delete=True)
            zvol_manage_queue = method_frame.method.queue

            # Turn on delivery confirmations
            channel.confirm_delivery()

            message = {'action': 'set_zvol', 'zvol': volume, 'hosting': hosting, 'size': size}

            def on_message(channel, method_frame, header_frame, body):
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                message = json.loads(body)
                if(message['status'] == 'success'):
                    self.block_dev = message['bdev']
                else:
                    error_msg = ''
                    if 'error_description' in message:
                        error_msg = message['error_description']
                    self.abort('%s %s'%(message['error'], error_msg))
                channel.stop_consuming()
                channel.close()


            # Send a message
            if channel.basic_publish(exchange='rocks.vm-manage',
                                     routing_key=nas,
                                     mandatory=True,
                                     body=json.dumps(message, ensure_ascii=False),
                                     properties=pika.BasicProperties(content_type='application/json',
                                                                     delivery_mode=1,
                                                                     correlation_id = str(uuid.uuid4()),
                                                                     reply_to = zvol_manage_queue
                                                                    )
                                    ):
                channel.basic_consume(on_message, zvol_manage_queue)
                channel.start_consuming()
                
                return self.block_dev
            else:
                self.abort('Message could not be delivered: ')

        def run(self, params, args):
                (args, nas, volume, hosting, size) = self.fillPositionalArgs(
                                ('nas', 'volume', 'hosting', 'size'))

                if not nas or not volume or not hosting or not size:
                        self.abort("you must pass 4 arguments nas_name volume hosting size")


                # debugging output
                print "mapping ", nas, ":", volume, " on ", hosting
                device = self.callAddHostStoragemap(nas, volume, hosting, size)
                self.beginOutput()
                self.addOutput(nas, device)
                self.endOutput(padChar='')



RollName = "kvm"
