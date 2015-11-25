
import json

class NodeConfig:
	"""Simple JSON configuration file for imgstorage vm """
	def __init__(self,config="/etc/imgstorage.conf"):
		self.CONFFILE=config
		with open(self.CONFFILE) as conf:
			data = json.load(conf)[0]
			self.NODE_NAME = data['name']
			self.SYNC_NETWORK = data['network']
			self.VM_CONTAINER_ZPOOL = data['default_pool']
			self.IMG_SYNC_WORKERS = data['img_sync_workers']
			self.DATA = data
