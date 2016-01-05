#
# @Copyright@
# 
# 				Rocks(r)
# 		         www.rocksclusters.org
# 		         version 6.2 (SideWinder)
# 
# Copyright (c) 2000 - 2014 The Regents of the University of California.
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
import rocks.commands
import string

class Command(rocks.commands.HostArgumentProcessor,
	rocks.commands.report.command):
	"""
	create /etc/imgstorage.conf	

	<arg optional='0' type='string' name='host'>
	Host name of machine
	</arg>

	<param optional='1' type='string' name='network'>
	Use a different network name for synchronization/iscsi
	Traffic. Defaults to the dnszone of the rocks private network
	</param>

	<param optional='1' type='string' name='pool'>
	Define the name of the pool to use for local volumes.
	Overrides host attribute imgstorage_pool 
	</param>

	<param optional='1' type='string' name='workers'>
	set the number of synchronization workers overriding both the default (8) and
	host attribute imgstorage_workers (if it exists)
	</param>
	
	<example cmd='report host imgstorage vm-container-0-0'>
	Create the image storage configuration file for vm-container-0-0.
	</example>

	<example cmd='report host imgstorage vm-container-0-0 network=fast'>
	Create the image storage configuration file for vm-container-0-0 but use
	vm-container-0-0.fast as the interface for iscsi/synchronization traffic
	</example>
	"""

	def run(self, params, args):

		DEFAULT_WORKERS = 8
		network,default_pool,sync_workers, = self.fillParams([
			('network', None),
			('pool', None),
			('workers', None)
			])

		self.beginOutput()
		for host in self.getHostnames(args):
			private = self.command('list.network', 
				['private','output-col=dnszone', 'output-header=no']).strip()
			if network is None:
				network = self.db.getHostAttr(host,'imgstorage-network')
			network = private if network is None else network

		
			if default_pool is None:
				default_pool = self.db.getHostAttr(host,'imgstorage-pool')
			old_pool = self.db.getHostAttr(host,'vm_container_zpool')
			default_pool = old_pool if default_pool is None else default_pool

			
			if sync_workers is None:
				sync_workers = self.db.getHostAttr(host,'imgstorage-workers')
			sync_workers = DEFAULT_WORKERS if sync_workers is None else sync_workers
			
			self.addOutput(host, '<file name="/etc/imgstorage.conf">')
			self.addOutput(host, '[ {')
			self.addOutput(host, '"name" : "%s",' % host)
			self.addOutput(host, '"network" : "%s",' % network)
			self.addOutput(host, '"default_pool" : "%s",' % default_pool)
			self.addOutput(host, '"img_sync_workers" : "%s"' % sync_workers)
			self.addOutput(host, '} ]')
			self.addOutput(host,'</file>')

		self.endOutput(padChar='')
