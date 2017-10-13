NAME = img-storage-vm
RELEASE = 2
RPM.ARCH	= noarch
RPM.FILES = \
/etc/rc.d/init.d/* \n \
/etc/systemd/system/* \n \
/opt/rocks/bin/* \n \
$(PY.ROCKS)/*egg-info
