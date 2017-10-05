NAME = img-storage
RELEASE = 3
RPM.ARCH	= noarch
ifeq ($(ROCKS_VERSION_MAJOR),"6")
PV_RPM 6/$(ARCH)/pv-1.1.4-3.el6.x86_64.rpm
else
PV_RPM = 7/$(ARCH)/pv-1.4.6-1.el7.x86_64.rpm
endif
RPM.FILES = \
$(PY.ROCKS)/*egg-info \n \
$(PY.ROCKS)/imgstorage

