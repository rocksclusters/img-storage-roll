#!/opt/python/bin/python
import subprocess
import sys
import time

iscsi_name = sys.argv[1]

def create_zvol(zvol_name):
	return subprocess.call(['zfs', 'create', '-V', '10gb', zvol_name]) == 0

def create_tgt(tgt_name):
	cmd = subprocess.Popen(['tgt-setup-lun', '-n', tgt_name, '-d', '/dev/tank/%s'%tgt_name], stdout=subprocess.PIPE)
	for line in cmd.stdout:
    		if "Creating new target" in line:
        		return line['Creating new target (name='.__len__():line.index(',')]

def remove_tgt(tgt_name):
	cmd = subprocess.Popen(['tgtadm', '--op', 'show', '--mode', 'target'], stdout=subprocess.PIPE)
	for line in cmd.stdout:
		if line.startswith('Target '):
			if line.split()[2] == tgt_name:
				tgt_num = line.split()[1]
				return subprocess.call(['tgtadm', '--lld', 'iscsi', '--op', 'delete', '--mode', 'target', '--tid', tgt_num]) == 0
	return False

def remove_zvol(zfs_vol):
	return subprocess.call(['zfs', 'destroy', zfs_vol]) == 0

def connect_iscsi(iscsi_target, node_name):
	cmd = subprocess.Popen(['iscsiadm', '--mode', 'discovery', '--type', 'sendtargets', '-p', node_name], stdout=subprocess.PIPE)
	for line in cmd.stdout:
		if iscsi_target in line: #has the target
			return subprocess.call(['iscsiadm', '-m', 'node', '-T', iscsi_target, '-p', node_name, '-l']) == 0
	return False

def disconnect_iscsi(iscsi_target):
	return subprocess.call(['iscsiadm', '-m', 'session', '--logout']) == 0


print "Creating zvol %s: %s"%(iscsi_name, create_zvol("tank/%s"%iscsi_name))
iscsi_target = create_tgt(iscsi_name)
print "Created tgt %s"%(iscsi_target)
print "Connecting to iscsi %s"%connect_iscsi(iscsi_target, '10.1.255.2:3260')
print "Disconnecting from iscsi %s"%disconnect_iscsi(iscsi_target)
print "Removing tgt %s: %s"%(iscsi_target, remove_tgt(iscsi_target))
time.sleep(3)
print "Removing zfs_vol %s: %s"%(iscsi_name, remove_zvol("tank/%s"%iscsi_name))
