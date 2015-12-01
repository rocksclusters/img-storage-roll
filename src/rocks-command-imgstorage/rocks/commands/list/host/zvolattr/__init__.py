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
    List the attributes of a zvol located on a NAS (or virtual machine images repository).
    
    <arg type='string' name='nas' optional='0'>
    The NAS name which we want to interrogate
    </arg>

    <arg type='string' name='zvol' optional='0'>
    The name of the volume to interrogate 
    </arg>

    <example cmd='list host zvolattr nas-0-0 hosted-vm-0-0-0-vol'>
    Display the attributes of the zvol named hosted-vm-0-0-0-vol on nas-0-0
    </example>
    """

    def run(self, params, args):
        (args, nas, zvol) = self.fillPositionalArgs(('nas','zvol'))

        if not zvol:
            self.abort("you must enter the zvol  name")
	self.beginOutput()
        attrs = CommandLauncher().callListZvolAttrs(nas,zvol)
	fields = ['zvol','frequency','nextsync','uploadspeed','downloadspeed']
	line = []
	for f in fields:
		line.extend([str(attrs[f])])
        self.addOutput(nas, line)
        headers=['nas']
	headers.extend(fields)
        self.endOutput(headers)
