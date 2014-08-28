#!/opt/rocks/bin/python
#
#

import sqlalchemy
from sqlalchemy import *

from rocks.db.mappings.base import Base, RocksBase
# we need this for the backref
import rocks.db.mappings.kvm


class ImgNasServer(RocksBase, Base):

	__tablename__ = 'img_nas_server'

	ID = Column('ID', Integer, primary_key=True, nullable=False)
	disk_ID = Column('vm_disk_id', Integer, ForeignKey('vm_disks.ID'),
			nullable=False, server_default='0')
	server_name = Column('nas_name', String(64), server_default='')
	zpool_name = Column('zpool_name', String(128), server_default='')

	disk = sqlalchemy.orm.relationship("VmDisk", uselist=False, 
		backref=sqlalchemy.orm.backref("img_nas_server", uselist=False),)



