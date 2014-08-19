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


class Command(rocks.commands.HostArgumentProcessor, rocks.commands.list.command):
        """
        List the current active mapping on a NAS (or virtual machine images 
        repository).
        
        <arg type='string' name='nas' optional='0'>
        The NAS name which we want to interrogate
        </arg>

        <example cmd='list host storagemap nas-0-0'>
        It will display the list of mappings on nas-0-0
        </example>
        """

        def run(self, params, args):
                (args, nas) = self.fillPositionalArgs(('nas'))

                if not nas:
                        self.abort("you must enter the nas name")
                # debugging output
                list = CommandLauncher().callListHostStatus(nas)
                self.beginOutput()
                for d in list:
                    self.addOutput(nas, d.values())
                headers=['nas','is sending', 'target', 'zvol', 'time started']
                self.endOutput(headers)





RollName = "img-storage"
