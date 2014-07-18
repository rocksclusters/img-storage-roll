
import subprocess

class ActionError(Exception):
    def __init__(self, message, description=None):
        Exception.__init__(self, message)
        self.descripton = description

def runCommand(params):
    cmd = subprocess.Popen(params, stdout=subprocess.PIPE, stderr = subprocess.PIPE)
    out, err = cmd.communicate()
    if cmd.returncode:
        raise ActionError('Error executing %s: %s'%(params[0], err))
    else:
        return out.splitlines() 
