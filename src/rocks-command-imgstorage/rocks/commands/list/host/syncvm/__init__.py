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
        List the current sync queue on a VM
        
        <arg type='string' name='vm' optional='0'>
        The VM container name which we want to interrogate
        </arg>

        <example cmd='list host storagemap compute-0-0'>
        It will display the list of sync tasks on compute-0-0
        </example>
        """

        def run(self, params, args):
                (args, vm) = self.fillPositionalArgs(('vm'))

                if not vm:
                        self.abort("you must enter the vm name")
                # debugging output
                list = CommandLauncher().callListHostSync(vm)
                self.beginOutput()
                for d in list:
                    self.addOutput(vm, (
                        d['zvol'],
                        d['iscsi_target'],
                        d['started'],
                        str(datetime.timedelta(seconds=(int(time.time()-d['time']))))
                    ))
                headers=['vm','zvol', 'target', 'is started', 'time']
                self.endOutput(headers)





RollName = "img-storage"
