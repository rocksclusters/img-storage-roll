#!/opt/rocks/bin/python
# @Copyright@
#
#                               Rocks(r)
#                        www.rocksclusters.org
#                        version 5.6 (Emerald Boa)
#                        version 6.1 (Emerald Boa)
#
# Copyright (c) 2000 - 2013 The Regents of the University of California.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# 1. Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
# notice unmodified and in its entirety, this list of conditions and the
# following disclaimer in the documentation and/or other materials provided
# with the distribution.
#
# 3. All advertising and press materials, printed or electronic, mentioning
# features or use of this software must display the following acknowledgement:
#
#       "This product includes software developed by the Rocks(r)
#       Cluster Group at the San Diego Supercomputer Center at the
#       University of California, San Diego and its contributors."
#
# 4. Except as permitted for the purposes of acknowledgment in paragraph 3,
# neither the name or logo of this software nor the names of its
# authors may be used to endorse or promote products derived from this
# software without specific prior written permission.  The name of the
# software includes the following terms, and any derivatives thereof:
# "Rocks", "Rocks Clusters", and "Avalanche Installer".  For licensing of
# the associated name, interested parties should contact Technology
# Transfer & Intellectual Property Services, University of California,
# San Diego, 9500 Gilman Drive, Mail Code 0910, La Jolla, CA 92093-0910,
# Ph: (858) 534-5815, FAX: (858) 534-7345, E-MAIL:invent@ucsd.edu
#
# THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS''
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
# IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# @Copyright@
#

import subprocess
import logging
import os
import rocks.db.helper

class ActionError(Exception):
    pass

class ZvolBusyActionError(ActionError):
    pass

""" Runs system command. If passed second command, the output of first one will be piped to second one """
def runCommand(params, params2 = None, shell=False):
    try:
        cmd = subprocess.Popen(params, stdout=subprocess.PIPE, stderr = subprocess.PIPE, shell=shell)
    except OSError, e:
        raise ActionError('Command %s failed: %s' % (params[0], str(e)))

    if params2:
        try:
            cmd2 = subprocess.Popen(params2, stdin=cmd.stdout, stdout=subprocess.PIPE, stderr = subprocess.PIPE, shell=shell)
        except OSError, e:
            raise ActionError('Command %s failed: %s' % (params2[0], str(e)))
        cmd.stdout.close()
        out, err = cmd2.communicate()
        if cmd2.returncode:
            raise ActionError('Error executing %s: %s'%(params2[0], err))
        else:
            return out.splitlines()

    else:
        out, err = cmd.communicate()
        if cmd.returncode:
            raise ActionError('Error executing %s: %s'%(params[0], err))
        else:
            return out.splitlines()

def setupLogger(logger):
    formatter = logging.Formatter("'%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s'")
    handler = logging.FileHandler("/var/log/rocks/img-storage.log")
    handler.setFormatter(formatter)

    #for log_name in (logger, 'pika.channel', 'pika.connection', 'rabbit_client.RabbitMQClient'):
    for log_name in ([logger, 'rabbit_client.RabbitMQCommonClient', 'tornado.application']):
        logging.getLogger(log_name).setLevel(logging.DEBUG)
        logging.getLogger(log_name).addHandler(handler)

    return handler


def get_attribute(attr_name, hostname, logger = None):
    """connect to the database and return the value of the for the given
    attr_name relative to the hostname"""
    try:
        db = rocks.db.helper.DatabaseHelper()
        db.connect()
        hostname = str(db.getHostname(hostname))
        #logger.debug('hostname %s attr_name %s' % (hostname, attr_name))
        value = db.getHostAttr(hostname, attr_name)
        return value
    except Exception, e:
        error = "Unable to get attribute %s for host %s (%s)" % \
                (attr_name, hostname, str(e))
	if logger:
            logger.exception(error)
        raise ActionError(error)
    finally:
        db.close()
        db.closeSession()


def isFileUsed(file):
	"""return true if file is in use otherwise false"""
	ret = os.system('fuser %s' % file)
	returnValue = ret >> 5
	if returnValue:
		return False
	else:
		# fuser fails if the file is unused
		return True

