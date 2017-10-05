NAME = img-storage-nas
RELEASE = 1
RPM.ARCH	= noarch
RPM.FILES = \
/etc/rc.d/init.d/* \n \
/opt/rocks/bin/* \n \
$(PY.ROCKS)/*egg-info
