#!/opt/rocks/bin/python
#
#

import sys
import string
import rocks.commands
from rabbit_client.CommandLauncher import CommandLauncher

class Command(rocks.commands.HostArgumentProcessor, rocks.commands.remove.command):
	"""
	Remove a storage volume form a NAS (or virtual machine images 
	repository).
	
	<arg type='string' name='nas' optional='0'>
	The NAS name which hosts the storage image
	</arg>

	<arg type='string' name='volume' optional='0'>
	The volume name which will be deleted
	</arg>

	<example cmd='remove host storageimg nas-0-0 zpool/vm-sdsc125-2'>
	It remove the volume zpool/vm-sdsc125-2 from nas-0-0
	</example>
	"""

	def run(self, params, args):
		(args, nas, volume) = self.fillPositionalArgs(
				('nas', 'volume'))

		# debugging output
		if not (nas and volume):
			self.abort("2 argument are required for this command nas volume")

		print "removing  ", nas, ":", volume
		CommandLauncher().callDelHostStorageimg(nas, volume)

		self.beginOutput()
		self.addOutput(nas, "Success")
		self.endOutput(padChar='')





RollName = "img-storage"
