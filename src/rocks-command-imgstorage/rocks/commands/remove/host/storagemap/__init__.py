#!/opt/rocks/bin/python
#
#

import sys
import string
import rocks.commands


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

	def tear_down(self, nas, volume):
		# return an string with error if there are errors
		return ""


	def run(self, params, args):
		(args, nas, volume) = self.fillPositionalArgs(
				('nas', 'volume'))

		# debugging output
		if not (nas and volume):
			self.abort("2 argument are required for this command nas volume")

		# debugging output
		print "unmapping  ", nas, ":", volume
		device = self.tear_down(nas, volume)

		if device:
			self.beginOutput()
			self.addOutput(nas, device)
			self.endOutput(padChar='')




RollName = "kvm"
