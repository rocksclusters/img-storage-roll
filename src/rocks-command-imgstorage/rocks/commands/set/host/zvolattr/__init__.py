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

    <arg type='string' name='zvol' optional='0'>
    zvol for which the the attribute should be set 
    </arg>

    <arg type='string' name='attr' optional='0'>
    Attribute to set 
    </arg>

    <arg type='string' name='value' optional='0'>
    Value to set named attribute.  'None' will set it to a python
    None value 
    </arg>

    <example cmd='set host zvolattr nas-0-0 hosted-vm-0-0-0-vol frequency 900'>
    Set the frequency of synchronization for zvol hosted-vm-0-0-0-vol to 900 seconds 
    </example>

    <example cmd='set host zvolattr nas-0-0 hosted-vm-0-0-0-vol frequency None'>
    set frequency to the default system attribute 
    </example>
    """

    def run(self, params, args):

	if len(args) != 4:
		self.abort('Must supply at (nas,zvol,attr,value) tuple')
	(args,nas,zvol,attr,value) = self.fillPositionalArgs(('nas','zvol','attr','value'))
	if value.lower() == "none":
		value = None
	setDict = {attr:value}
       	CommandLauncher().callSetZvolAttrs(nas,zvol,setDict)
