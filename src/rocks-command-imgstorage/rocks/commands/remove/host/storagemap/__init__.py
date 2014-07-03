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



class Command(rocks.commands.HostArgumentProcessor, rocks.commands.remove.command):
	"""
	Remove a mapping between a virtual machine image from the NAS (or virtual machine images 
	repository) to the hosting environment.
	
	<arg type='string' name='nas' optional='0'>
	The NAS name which will host the storage image
	</arg>

	<arg type='string' name='volume' optional='0'>
	The volume name which will be unmapped on the hosting environment
	</arg>

	<example cmd='remove host storagemap nas-0-0 zpool/vm-sdsc125-2'>
	It removes the existing mapping on nas-0-0 zpool/vm-sdsc125-2
	compute-0-0-0.
	</example>
	"""

	def callDelHostStoragemap(self, nas, volume):
	    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))

	    channel = connection.channel()

	    # Declare the queue
	    method_frame = channel.queue_declare(exclusive=True, auto_delete=True)
	    zvol_manage_queue = method_frame.method.queue

	    # Turn on delivery confirmations
	    channel.confirm_delivery()

	    message = {'action': 'tear_down', 'zvol': volume}

	    def on_message(channel, method_frame, header_frame, body):
		channel.basic_ack(delivery_tag=method_frame.delivery_tag)
		channel.stop_consuming()
		channel.close()
		print body
		message = json.loads(body)
		if(message['status'] == 'success'):
		    self.beginOutput()
		    self.addOutput("Done ", "Done")
		    self.endOutput(padChar='')
		else:
		    self.abort(message['error'])


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
	    else:
                self.abort('Message could not be delivered')

	def run(self, params, args):
		(args, nas, volume) = self.fillPositionalArgs(
				('nas', 'volume'))

		# debugging output
		if not (nas and volume):
			self.abort("2 argument are required for this command nas volume")

		# debugging output
		print "unmapping  ", nas, ":", volume
		self.callDelHostStoragemap(nas, volume)

RollName = "kvm"
