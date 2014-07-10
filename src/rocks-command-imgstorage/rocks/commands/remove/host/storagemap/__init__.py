#!/opt/rocks/bin/python
#
#

import sys
import string
import rocks.commands

from rabbit_client.CommandLauncher import CommandLauncher

class Command(rocks.commands.HostArgumentProcessor, rocks.commands.remove.command):
        """
        Remove a mapping between a virtual machine image from the NAS (or virtual machine images 
        repository) to the hosting environment.
        
        <arg type='string' name='nas' optional='0'>
        The NAS name which will host the storage image
        </arg>

        <arg type='string' name='volume' optional='0'>
        The volume name which will be unmapped on the hosting environment
        </arg>

        <example cmd='remove host storagemap nas-0-0 zpool/vm-sdsc125-2'>
        It removes the existing mapping on nas-0-0 zpool/vm-sdsc125-2
        compute-0-0-0.
        </example>
        """


        def run(self, params, args):
                (args, nas, volume) = self.fillPositionalArgs(
                                ('nas', 'volume'))

                # debugging output
                if not (nas and volume):
                        self.abort("2 argument are required for this command nas volume")

                # debugging output
                print "unmapping  ", nas, ":", volume
                CommandLauncher().callDelHostStoragemap(nas, volume)

                self.beginOutput()
                self.addOutput(nas, "Success")
                self.endOutput(padChar='')
RollName = "kvm"
