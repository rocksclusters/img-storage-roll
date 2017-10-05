NAME			= img-storage-pw-plugin
VERSION			= 1
RELEASE			= 1
PLUGIN			= img-storage_pw.py
PLUGINDIR		= /opt/rocks/var/plugins/sec_attr
RPM.REQUIRES		= rocks-secattr-plugins
RPM.FILES		= $(PLUGINDIR)/*
