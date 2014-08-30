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
    List the current active mapping on a NAS (or virtual machine images 
    repository).
    
    <arg type='string' name='nas' optional='0'>
    The NAS name which we want to interrogate
    </arg>

    <example cmd='list host storagemap nas-0-0'>
    It will display the list of mappings on nas-0-0
    </example>
    """

    #def list(self, nas):
        # Im not too sure what is the best way to implement this
        # what is the right set of api

        # return list of tuple in the form of (zvolname, mappedhost, devicename)
        #return [("zpool/vm-sdsc125-2","compute-0-0","/dev/sdc"), 
        #   ("zpool/vm-sdsc125-3","compute-0-1","/dev/sdc")]


    def run(self, params, args):
        (args, nas) = self.fillPositionalArgs(('nas'))

        if not nas:
            self.abort("you must enter the nas name")
        # debugging output
        list = CommandLauncher().callListHostStoragemap(nas)
        self.beginOutput()
        for d in list:
            state = 'mapped'
            if(d['remotehost'] == None):
                state = 'unmapped'
            elif(d['is_sending'] == 1):
                state = 'NAS⇒ VM'
            elif(d['is_sending'] == 0):
                state = 'NAS⇐ VM'
                if(d['is_delete_remote'] == 0):
                    state += ' sched'

            self.addOutput(nas, (
                d['zvol'],
                d['remotehost'],
                d['zpool'],
                d['iscsi_target'],
                state,
                str(datetime.timedelta(seconds=(int(time.time()-d.get('time'))))) if d.get('time') else None
                ))
        headers=['nas', 'zvol', 'host', 'zpool', 'target', 'state', 'time']
        self.endOutput(headers)





RollName = "img-storage"
