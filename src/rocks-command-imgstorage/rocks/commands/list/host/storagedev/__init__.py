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
    List the current mapped blockdevs to iscsi targets on compute
    
    <arg type='string' name='compute' optional='0'>
    The COMPUTE name which we want to interrogate
    </arg>

    <example cmd='list host storagemap compute-0-0'>
    It will display the list of mappings on compute-0-0
    </example>
    """

    def run(self, params, args):
        (args, compute) = self.fillPositionalArgs(('compute'))

        if not compute:
            self.abort("you must enter the compute name")

        response = CommandLauncher().callListHostStoragedev(compute)
        if(response['node_type'] == 'iscsi'):
            self.beginOutput()
            for d in response['body']:
                self.addOutput(compute, d.values())
            headers=['compute','target', 'device']
            self.endOutput(headers)
        elif(response['node_type'] == 'sync'):
            self.beginOutput()
            map = response['body']
            for d in map.keys():
                self.addOutput(compute, (d, 
                        map[d].get('dev'),
                        map[d].get('status'), 
                        map[d].get('size'), 
                        #map[d].get('target'),
                        map[d].get('bdev'),
                        map[d].get('started'),
                        map[d].get('synced'),
                        str(datetime.timedelta(seconds=(int(time.time()-map[d].get('time'))))) if map[d].get('time') else None
                    )
                )
            headers=['compute','zvol','lvm','status','size (GB)','block dev','is started','synced','time']
            self.endOutput(headers)




RollName = "img-storage"
