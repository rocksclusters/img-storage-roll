#!/opt/rocks/bin/python
#
# @Copyright@
#
# 				Rocks(r)
# 		         www.rocksclusters.org
# 		         version 5.6 (Emerald Boa)
# 		         version 6.1 (Emerald Boa)
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
# 	"This product includes software developed by the Rocks(r)
# 	Cluster Group at the San Diego Supercomputer Center at the
# 	University of California, San Diego and its contributors."
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
import sys
import string
import pika
import time
import json
import uuid
from rocks.util import CommandError
import logging
logging.basicConfig()
from rabbit_client.RabbitMQClient import RabbitMQLocator

class CommandLauncher():

    def __init__(self):
       self.RABBITMQ_URL = RabbitMQLocator().RABBITMQ_URL
       self.ret_message = None

    def callAddHostStoragemap(self, nas, volume, hosting, size):
       message = {'action': 'set_zvol', 'zvol': volume, 'hosting': hosting, 'size': size}
       self.callCommand(message, nas)
       block_dev = self.ret_message['bdev']
       return block_dev

    def callDelHostStoragemap(self, nas, volume):
       message = {'action': 'tear_down', 'zvol': volume}
       self.callCommand(message, nas)
       return

    def callDelHostStorageimg(self, nas, volume):
       message = {'action': 'del_zvol', 'zvol': volume}
       self.callCommand(message, nas)
       return

    def callListHostStoragemap(self, nas):
       message = {'action': 'list_zvols'}
       self.callCommand(message, nas)
       return self.ret_message['body']

    def callListHostStoragedev(self, compute):
       message = {'action': 'list_dev'}
       self.callCommand(message, compute)
       return self.ret_message['dev_list']



    def callCommand(self, message, nas):
        connection = pika.BlockingConnection(pika.URLParameters(self.RABBITMQ_URL))

        channel = connection.channel()
        #channel.add_on_close_callback(lambda channel, replyCode, replyText: print("!!!!!!!!!!!!!!"))

        try:
            # Declare the queue
            method_frame = channel.queue_declare(exclusive=True, auto_delete=True)
            zvol_manage_queue = method_frame.method.queue
            channel.confirm_delivery()

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
                channel.basic_consume(self.on_message, zvol_manage_queue)
                channel.start_consuming()
                if self.ret_message['status'] == 'error':
                    if('error' in self.ret_message.keys()):
                        raise CommandError(self.ret_message['error'])
                    else
                        raise CommandError('Error occured')
                return
            else:
                raise CommandError('Message could not be delivered')
        finally:
           channel.close()


    def on_message(self, channel, method_frame, header_frame, body):
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        channel.stop_consuming()
        self.ret_message = json.loads(body)

