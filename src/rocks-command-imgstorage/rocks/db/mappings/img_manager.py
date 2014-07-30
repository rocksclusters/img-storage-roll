#!/opt/rocks/bin/python
#
#


from sqlalchemy import *

from rocks.db.mappings.base import *


class ImgNasServer(RocksBase, Base):

	__tablename__ = 'img_nas_server'

	ID = Column('ID', Integer, primary_key=True, nullable=False)
	disk_ID = Column('vm_disk_id', Integer, ForeignKey('vm_disks.ID'),
								nullable=False, default=0)
	server_name = Column('Name', String(32))

	disk = sqlalchemy.orm.relationship("VmDisk", uselist=False, 
				backref=sqlalchemy.orm.backref("img_nas_server", uselist=False),)



