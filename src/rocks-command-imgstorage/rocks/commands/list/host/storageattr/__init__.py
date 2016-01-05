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

from imgstorage.commandlauncher import CommandLauncher

import time
import datetime

class Command(rocks.commands.HostArgumentProcessor, rocks.commands.list.command):
    """
    List the global attributes for the daemon running on a  NAS (or virtual machine images repository).
    
    <arg type='string' name='nas' optional='0' repeat='1'>
    one or more NASes which we want to interrogate
    </arg>

    <example cmd='list host storageattr nas-0-0'>
    Display the global attributes on nas-0-0
    </example>
    """

    def run(self, params, args):

	if len(args) == 0:
		self.abort('Must supply at least one host name')
	self.beginOutput()
	for host in self.newdb.getNodesfromNames(args, preload=['membership']):
        	attrs = CommandLauncher().callListAttrs(host.name)
		for k in attrs.keys():
        		self.addOutput(host.name, (k,attrs[k]))
        headers=['nas','attr','value']
        self.endOutput(headers)
