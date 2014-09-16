#!/opt/rocks/bin/python
#
#

import sys
import string
import os
import rocks.commands

import imgstorage
import imgstorage.imgstoragevm

class Command(rocks.commands.HostArgumentProcessor, rocks.commands.Command):
	"""
	Clean a mapping between a virtual machine image from the NAS (or virtual machine images 
	repository) to the hosting environment.
	
	<arg type='string' name='nas' optional='0'>
	The NAS name which will host the storage image
	</arg>

	<arg type='string' name='volume' optional='0'>
	The volume name which will be unmapped on the hosting environment
	</arg>

	<example cmd='remove host storagemap nas-0-0 vm-sdsc125-2'>
	It removes the existing mapping on nas-0-0 vm-sdsc125-2
	compute-0-0-0.
	</example>
	"""


	def run(self, params, args):
		(args, nas, volume) = self.fillPositionalArgs(
				('nas', 'volume'))

		if not (nas and volume):
			self.abort("2 arguments are required for this " + \
					"command nas and volume")


		# ISCSI target removal
		mappings = imgstorage.imgstoragevm.get_blk_dev_list()
		target_name = nas + '-' + volume
		def func(x): return x.endswith(target_name)
		target = self.find_one(mappings, func, target_name)
		if target:
			print "removing ISCSI target ", target
			imgstorage.imgstoragevm.disconnect_iscsi(target)


		# device mapper removal
		dm_name = '%s-snap' % volume
		dm_path = '/dev/mapper/' + dm_name
		if os.path.exists(dm_path):
			print 'removing dm ', dm_name
			imgstorage.runCommand(['dmsetup', 'remove', dm_name])


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
			imgstorage.runCommand(['zfs', 'destroy', '-r', target])



	def find_one(self, zfs_list, func, name):
		"""return a list containing only 1 element with the matched entry, 
		if there are not matches it returns an empty list if there are 
		more then one matches it aborts"""

		t = filter(func, zfs_list)
		if len(t) > 1:
			self.abort('found multiple entries matching %s (%s)'\
				% (name, ', '.join(t)))
		if len(t) == 1:
			return t
		else:
			return []





RollName = "img-storage"
