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
        List the current sync queue on a NAS
        
        <arg type='string' name='nas' optional='0'>
        The NAS name which we want to interrogate
        </arg>

        <example cmd='list host storagemap nas-0-0'>
        It will display the list of sync tasks on nas-0-0
        </example>
        """

        def run(self, params, args):
                (args, nas) = self.fillPositionalArgs(('nas'))

                if not nas:
                        self.abort("you must enter the nas name")
                # debugging output
                list = CommandLauncher().callListHostSync(nas)
                self.beginOutput()
                for d in list:
                    self.addOutput(nas, (
                        d['remotehost'],
                        "upload" if d['is_sending'] else "download",
                        d['zvol'],
                        str(datetime.timedelta(seconds=(int(time.time()-d['time']))))
                    ))
                headers=['nas','remote host', 'action', 'zvol', 'time']
                self.endOutput(headers)





RollName = "img-storage"
