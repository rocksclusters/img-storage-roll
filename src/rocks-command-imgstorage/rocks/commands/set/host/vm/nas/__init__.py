#!/opt/rocks/bin/python
# 
# @Copyright@
# 
# 				Rocks(r)
# 		         www.rocksclusters.org
# 		         version 6.2 (SideWinder)
# 		         version 7.0 (Manzanita)
# 
# Copyright (c) 2000 - 2017 The Regents of the University of California.
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
#

import os.path
import random
import rocks.commands
import rocks.db.mappings.img_manager
from rocks.db.mappings.img_manager import ImgNasServer


class Command(rocks.commands.HostArgumentProcessor, rocks.commands.set.command):
	"""
	Set the external nas used by a virtual host for its disk

	<arg type='string' name='host' optional='0'>
	One or more VM host names.
	</arg>

	<param type='string' name='nas'>
	The hostname of the nas that will host the disk image of this machine
	If nas='none', it resets both the NAS name and the zpool name, this
	means that this virtual disk will use the value specified in the
	rocks list host vm showdisks=1 table
	</param>

	<param type='string' name='zpool'>
	The zpool name on which this disk image is placed
	</param>

	<param type='string' name='index'>
	Not implemented yet. We supports remote disk image only on the first disk
	</param>

	<example cmd='set host vm nas compute-0-0-0 nas=nas-0-5'>
	use nas-0-5 to store compute-0-0-0 disk images 
	</example>
	"""

	def run(self, params, args):

		(nas, index, zpool) = self.fillParams([ ('nas', ""),
					('index', '0'), ('zpool', '')])

		try:
			index = int(index)
		except:
			self.abort("index must be an integer")

		if nas.lower() == 'none':
			nas = ""
			zpool = ""
		else:
			if nas not in self.newdb.getListHostnames():
				self.abort('nas %s must be a valid rocks host' \
					% nas)
			if not zpool:
				# comma is a non valid character for a zpool name
				zpool = self.newdb.getHostAttr(nas,'img_zpools')
				if not zpool:
					self.abort('you need to specify a zpool '
						'parameter or set the '
						'img_zpools attribute')
				zpool = zpool.split(',')
				zpool = random.choice(zpool)



		nodes = self.newdb.getNodesfromNames(args, preload=['vm_defs', \
					'vm_defs.disks', \
					'vm_defs.disks.img_nas_server'])
		for node in nodes:
			if not node.vm_defs or not node.vm_defs.disks \
				or not len(node.vm_defs.disks) >= index:
				self.abort("node %s is not a virtual node" \
						% node.name)

			#ok we are good to go
			disk = node.vm_defs.disks[index]
			if disk.img_nas_server:
				disk.img_nas_server.server_name = nas
				disk.img_nas_server.zpool_name = zpool
			else:
				# we need to create it
				nas_server = ImgNasServer(server_name = nas,
						zpool_name = zpool,
						disk = disk)
				self.newdb.getSession().add(nas_server)



RollName = "img-storage"
