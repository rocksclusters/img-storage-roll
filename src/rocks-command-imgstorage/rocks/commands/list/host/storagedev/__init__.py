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

from rabbit_client.CommandLauncher import CommandLauncher


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
                # debugging output
                list = CommandLauncher().callListHostStoragedev(compute)
                self.beginOutput()
                for d in list:
                    self.addOutput(compute, d.values())
                headers=['compute','target', 'device']
                self.endOutput(headers)




RollName = "kvm"
