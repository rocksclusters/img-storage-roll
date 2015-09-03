# set the rabbitmq_img-storage.conf password 

import rocks.commands

import os, sys
import tempfile

class plugin(rocks.commands.sec_attr_plugin):
	def get_sec_attr(self):
		return 'img-storage_pw'

	def filter(self, value):
		pwfile='/opt/rocks/etc/rabbitmq_img-storage.conf'
		# create a temporary file
		tf, tfname= tempfile.mkstemp()
		os.write(tf,value)
		os.close(tf)
		os.chmod(tfname,0400)	
		# Move temporary file to destination 
		import shutil
		shutil.move(tfname, pwfile)
