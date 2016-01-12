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

class Command(rocks.commands.HostArgumentProcessor, rocks.commands.set.command):
    """
   Set a global attribute for the daemon running on a  NAS (or virtual machine images repository).
    
    <arg type='string' name='nas' optional='0'>
    NAS on  which we want to set an attribute 
    </arg>

    <arg type='string' name='attr' optional='0'>
    Attribute to set 
    </arg>

    <arg type='string' name='value' optional='0'>
    Value to set named attribute.  'None' will set it to a python
    None value 
    </arg>

    <example cmd='set host storageattr nas-0-0 img_sync_workers 8'>
    set img_sync_workers to the string '8' 
    </example>

    <example cmd='set host storageattr nas-0-0 myAttr None'>
    set myAttr to the Python None type 
    </example>
    """

    def run(self, params, args):

	if len(args) != 3:
		self.abort('Must supply at (nas,attr,value) tuple')
	(args,nas,attr,value) = self.fillPositionalArgs(('nas','attr','value'))
	if value.lower() == "none":
		value = None
	setDict = {attr:value}
       	CommandLauncher().callSetAttrs(nas,setDict)
