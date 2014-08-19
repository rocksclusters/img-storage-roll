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
        List the current mapped virtual blockdevs on compute
        
        <arg type='string' name='compute' optional='0'>
        The COMPUTE name which we want to interrogate
        </arg>

        <example cmd='list host storagemap compute-0-0'>
        It will display the list of mappings on compute-0-0
        </example>
        """

        def run(self, params, args):
            self.beginOutput()
            for compute in self.getHostnames(args):
                # debugging output
                map = CommandLauncher().callListHostStoragevdev(compute)
                for d in map.keys():
                    self.addOutput(compute, (d, map[d]['status'], map[d]['size'], map[d].get('synced')))
                headers=['compute','device', 'status', 'size (GB)', 'synced']
            self.endOutput(headers)





RollName = "img-storage"
