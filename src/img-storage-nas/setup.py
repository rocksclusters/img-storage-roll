#!/opt/rocks/bin/python
#
#

from distutils.core import setup
from setuptools import find_packages
import os

version = os.environ.get('ROCKS_VERSION')

# 
# main configuration of distutils
# 
setup(
    name = 'img-storage-nas',
    version = version,
    description = 'Image Storage manager',
    author = 'Dmitry Mishin',
    author_email =  'dmitry.mishin@gmail.com',
    maintainer = 'Dmitry Mishin',
    maintainer_email =  'dmitry.mishin@gmail.com',
    platforms = ['linux'],
    url = 'https://rocksclusters.org',
    #long_description = long_description,
    #license = license,
    #main package, most of the code is inside here
    packages = find_packages(),
    #data_files = [('etc', ['etc/rocksrc'])],
    # disable zip installation
    zip_safe = False,
    #the command line called by users    
    scripts=['bin/img-storage-nas','bin/img-storage-sync'],
)
