#!/opt/rocks/bin/python
#
#

import sys
import string
import rocks.commands

from imgstorage.commandlauncher import CommandLauncher

class Command(rocks.commands.HostArgumentProcessor, rocks.commands.add.command):
	"""
	Map a virtual machine image from the NAS (or virtual machine images 
	repository) to the hosting environment.
	
	<arg type='string' name='nas' optional='0'>
	The NAS name which will host the storage image
	</arg>

	<arg type='string' name='zpool' optional='0'>
	The zpool name. The final full zvol path name will be formed as
	zpool + "/" + volume
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

	<arg type='bool' name='img_sync' optional='1'>
	Should the volume sync to the local (hosting) system or be an 
        iscsi-only remote mount.  Default: uses remote host\'s 
	img_sync attribute
	</arg>

	<example cmd='add host storagemap nas-0-0 tank vm-sdsc125-2 vm-container-0-0 35'>
	If it does not exist create tank/vm-sdsc125-2 on nas and map it to 
	vm-container-0-0.
	</example>
	<example cmd='add host storagemap nas-0-0 tank vm-sdsc125-2 vm-container 35 img_sync=no'>
	If it does not exist create tank/vm-sdsc125-2 on nas and map it to vm-container-0-0. 
	Do NOT sync the image (iscsi mount only)
	</example>
	"""

	def run(self, params, args):
		(args, nas, zpool, volume, hosting, size) = self.fillPositionalArgs(
				('nas', 'zpool', 'volume', 'hosting', 'size'))

		sync = self.fillParams( [('img_sync', None)] )
	
		if not nas or not zpool or not volume or not hosting or not size:
			self.abort("you must pass 5 arguments nas_name zpool volume hosting size")
		
		if sync is None:
			sync =  self.db.getHostAttr(hosting,'img_sync')
		# debugging output
		print "mapping ", nas, ":", zpool, "/", volume, " on ", hosting
		device = CommandLauncher().callAddHostStoragemap(nas, zpool, volume, hosting, size, 
			self.str2bool(sync) if sync is not None else False)
		self.beginOutput()
		self.addOutput(nas, device)
		self.endOutput(padChar='')


RollName = "img-storage"
