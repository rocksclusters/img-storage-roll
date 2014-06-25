#!/opt/rocks/bin/python
#
#

from distutils.core import setup
import os

# 
# main configuration of distutils
# 
setup(
    name = 'rocks-command-kvm',
    version = "0.1",
    description = 'Rocks KVM python library extension',
    author = 'Phil Papadopoulos',
    author_email =  'philip.papadopoulos@gmail.com',
    maintainer = 'Luca Clementi',
    maintainer_email =  'luca.clementi@gmail.com',
    platforms = ['linux'],
    url = 'http://www.rocksclusters.org',
    #long_description = long_description,
    #license = license,
    #main package, most of the code is inside here
    packages = [line.rstrip() for line in open('packages')],
    #data_files = [('etc', ['etc/rocksrc'])],
    # disable zip installation
    zip_safe = False,
    #the command line called by users    
)
