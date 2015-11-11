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
    Lists the VM container node status
    
    <arg type='string' name='host' optional='0'>
    The host (physical) name which we want to interrogate
    </arg>

    <example cmd='list host storagemap vm-container-0-0'>
    It will display the list of mappings on vm-container-0-0
    </example>
    """

    def run(self, params, args):
        (args, host) = self.fillPositionalArgs(('host'))

        if not host:
            self.abort("you must enter the host name")

        response = CommandLauncher().callListHostStoragedev(host)

        map = response['body']
        self.beginOutput()
        for volume in map.keys():
            self.addOutput(host, (volume, 
                    map[volume].get('sync'),
                    map[volume].get('target'),
                    map[volume].get('device'),
                    map[volume].get('status'), 
                    map[volume].get('size'), 
                    map[volume].get('bdev'),
                    map[volume].get('started'),
                    map[volume].get('synced'),
                    str(datetime.timedelta(seconds=(int(time.time()-map[volume].get('time'))))) if map[volume].get('time') else None
                )
            )
        headers=['host','volume','sync', 'target','device','status','size (GB)','block dev','is started','synced','time']
	self.endOutput(headers)

