#!/opt/rocks/bin/python
#
#

import sys
import string
import os
import rocks.commands

import imgstorage
from imgstorage.commandlauncher import CommandLauncher

class Command(rocks.commands.HostArgumentProcessor, rocks.commands.Command):
	"""
	Clean a mapping between a virtual machine image from the NAS (or
	virtual machine images repository) to the hosting environment.

	This command will wipe all the data that is left on the virtual
	machine container and return to the last saved state on the NAS.
	If you want to restore some date from the virtual machine
	container you should do it before running this command.

	<arg type='string' name='nas' optional='0'>
	The NAS name which will host the storage image
	</arg>

	<arg type='string' name='volume' optional='0'>
	The volume name which will be unmapped on the hosting
	environment
	</arg>

	<param type='string' name='nodetype'>
	Under normal circustances this paramers is not needed.
	But if you want to manually wipe storage mapping on
	a NAS or on a virtual machine container, you will have
	to ssh to the node and use this parameter with the value
	of nas or vmc depending on the node type you want to clear.
	</param>

	<example cmd='remove host storagemap nas-0-0 vm-sdsc125-2'>
	It removes the existing mapping on nas-0-0 vm-sdsc125-2
	compute-0-0-0.
	</example>
	"""


	def run(self, params, args):

		(args, nas, volume) = self.fillPositionalArgs(
				('nas', 'volume'))

                (nodetype, ) = self.fillParams([('nodetype', '')])



		if not (nas and volume):
			self.abort("2 arguments are required for this " + \
					"command nas and volume")


		if nodetype:
			# ok we have to clear either a vmc or a nas
			if nodetype == 'vmc' and \
				os.path.exists('/etc/init.d/img-storage-vm'):
				print " -- clearing VM Container --"
				self.clean_vm(nas, volume)

			elif nodetype == 'vmc':
				self.abort("/etc/init.d/img-storage-vm is missing, "
					"are you sure this is a virtual machine "
					"container")

			elif nodetype == 'nas' and \
				os.path.exists('/etc/init.d/img-storage-nas'):
				print " -- clearing NAS --"
				self.clean_nas(nas, volume)

			elif nodetype == 'nas':
				self.abort("/etc/init.d/img-storage-nas is missing, "
					"are you sure this is a nas enabled for "
					"serving VM disk images?")

			else:
				self.abort("nodetype can be only nas or vmc (%s)"
						% nodetype)

		else:
			#no nodetype specified so we need to query the nas
			list = CommandLauncher().callListHostStoragemap(nas)
			entry = [ d for d in list if d['zvol'] == volume]
			if len(entry) == 0:
				self.abort('Unable to find volume %s on nas %s'
						% (volume, nas))
			elif len(entry) > 1:
				self.abort('Major failure: found %d volumes with '
						'the same %s name' %
						(len(entry), volume))

			entry = entry[0]
			if entry['remotehost'] == None:
				self.abort('Volume %s is unmapped' % volume)

			if entry['is_sending'] == 1 or (entry['is_sending'] == 0 and d['is_delete_remote'] != 0):
				self.abort('Volume %s is currently getting transfered, please wait' % volume)


			# ok we are good to go, we can destroy the mapping
			# all possible error situation have been cleared
			# clear the remote host first
			cmdline = 'rocks clean host storagemap %s %s nodetype=' % (nas, volume)
			print "cleaning ", d['remotehost']
			self.command('run.host', [str(d['remotehost']), cmdline + 'vmc'])

			# then clean the nas
			print "cleaning ", nas
			self.command('run.host', [nas, cmdline + 'nas'])




	def clean_nas(self, nas, volume):
		"""clear disk mapping on a NAS node"""
		import imgstorage.imgstoragenas

		nasclient = imgstorage.imgstoragenas.NasDaemon()

		# iscsi target removal
		# zvol table cleanup
		targets = imgstorage.imgstoragenas.get_iscsi_targets()
		target = self.find_iscsi_target(targets, nas + '-' + volume)
		if target:
			target = target[0]
			print "removing ISCSI target ", target
			nasclient.detach_target(target, True)

		nasclient.clear_zvols_table(volume)

		# zvol_calls table cleanup
		print "clearing zvol_calls DB table"
		nasclient.release_zvol(volume)



	def clean_vm(self, nas, volume):
		import imgstorage.imgstoragevm

		# ISCSI target removal
		mappings = imgstorage.imgstoragevm.get_blk_dev_list()
		target_name = nas + '-' + volume
		def func(x): return x.endswith(target_name)
		target = self.find_one(mappings, func, target_name)
		if target:
			target = target[0]
			print "removing ISCSI target ", target
			imgstorage.imgstoragevm.disconnect_iscsi(target)


		# device mapper removal
		dm_name = '%s-snap' % volume
		dm_path = '/dev/mapper/' + dm_name
		if os.path.exists(dm_path):
			print 'removing dm ', dm_name
			imgstorage.runCommand(['dmsetup', 'remove',
					dm_name])


		# zfs FS removal
		targets = []
		zfs_list = imgstorage.imgstoragevm.get_zfs_list()
		vol_name = volume
		def func(x): return x.split('/')[-1] == vol_name
		targets.extend(self.find_one(zfs_list, func, vol_name))
		vol_name = volume + '-temp-write'
		targets.extend(self.find_one(zfs_list, func, vol_name))

		for target in targets:
			print "removing zfs volume ", target
			imgstorage.runCommand(['zfs', 'destroy', '-r',
					target])


	def find_iscsi_target(self, list, name):
		"""specializes self.find_one for iscsi target"""

		def func(x): return x.endswith(name)
		return self.find_one(list, func, name)


	def find_one(self, list, func, name):
		"""return a list containing only 1 element with the matched
		entry, if there are not matches it returns an empty list if
		there are more then one matches it aborts"""

		t = filter(func, list)
		if len(t) > 1:
			self.abort('found multiple entries matching %s (%s)'\
				% (name, ', '.join(t)))
		if len(t) == 1:
			return t
		else:
			return []





RollName = "img-storage"
