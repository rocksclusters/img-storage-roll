#!/opt/rocks/bin/python
#
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
from imgstorage.rabbitmqclient import RabbitMQLocator
from imgstorage import runCommand, ActionError, ZvolBusyActionError
import logging

from pysqlite2 import dbapi2 as sqlite3
import sys
import signal
import rocks.db.helper
import uuid

import logging
logging.basicConfig()

import getopt

def main(argv):
    logger = logging.getLogger(__name__)
    SQLITE_DB = '/opt/rocks/var/img_storage.db'

    if(confirm('Unmap iSCSI target?', True)):
        try:
            targets = runCommand(['tgtadm', '--op', 'show', '--mode', 'target']) 
            for line in targets:
                if line.startswith('Target '):
                    print line
            ans = raw_input("Which target number would you like to delete? (number) ")
            if(ans):
                iscsi_target = targets[int(ans)].split(' ')[1]
                logger.debug('Removing target %s'%iscsi_target)
                runCommand(['tgtadm', '--lld', 'iscsi', '--op', 'delete', '--mode', 'target', '--tid', ans])
        except ActionError, e: logger.exception(e)


    if(confirm('Remove zvol mapping to VM in DB?', True)):
        with sqlite3.connect(SQLITE_DB) as con:
            try:
                cur = con.cursor()
                cur.execute("SELECT * from zvols")
                linenum = 0
                rows = cur.fetchall()
                for row in rows:
                    print linenum, ' '.join([(x if x is not None else '') for x in row])
                    linenum+=1
                ans = raw_input("Which zvol? (name) ")
                if(ans):
                    cur.execute('UPDATE zvols SET iscsi_target = NULL, remotehost = NULL, zpool = NULL WHERE zvol = ?',[rows[int(ans)][0]])
                    con.commit()
                    print "Done"
            except ActionError, e: logger.exception(e)

    if(confirm('Unbusy the zvol?', True)):
        with sqlite3.connect(SQLITE_DB) as con:
            try:
                cur = con.cursor()
                cur.execute("SELECT * from zvol_calls")
                linenum = 0
                rows = cur.fetchall()
                for row in rows:
                    print linenum, ' '.join([(str(x) if x is not None else '') for x in row])
                    linenum+=1
                ans = raw_input("Which zvol? (number) ")
                if(ans):
                    cur.execute('DELETE FROM zvol_calls WHERE zvol = ?',[rows[int(ans)][0]])
                    con.commit()
            except ActionError, e: logger.exception(e)




def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.

    'resp' should be set to the default value assumed by the caller when
    user simply types ENTER.

    >>> confirm(prompt='Create Directory?', resp=True)
    Create Directory? [y]|n: 
    True
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: 
    False
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: y
    True

    """
    
    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')
        
    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print 'please enter y or n.'
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False


if __name__ == "__main__":
   main(sys.argv[1:])

