#solution 2 with Ronly remote iscsi target and write mapped to local storage on
#vm1-temp-write area when remote iscsi target is replicated locally VM is
#remapped to only local storage and local wirte temp area is merged in
#local virtual machine storage
#
#compute-0-1 active iscsi taget
#compute-0-0-0 vm with disk mapped to /dev/mapper/c00-snap
#compute-0-0-0 running on the FE calit2-119-100
#this script should be run on FE


## set up
## sdc is mapped with iscsi to remote target
zfs create -V 30g tank/vm1
zfs create -V 10g tank/vm1-temp-write
## setting up DM so that it sends all the write to local /dev/zvol/tank/vm1-temp-write and read from sdc
echo "0 62914560 snapshot /dev/sdc /dev/zvol/tank/vm1-temp-write P 16" | dmsetup create c00-snap
## /dev/mapper/c00-snap is what the VM should use as disk vm

rocks start host vm compute-0-0-0

echo run background copy of snapshot - no need to suspend device
ssh nas-0-1 zfs snap tank/vm-hpcdev-pub03-1-vol@test-final4
ssh nas-0-1 "zfs send tank/vm-hpcdev-pub03-1-vol@test-final4 |ssh compute-0-3 zfs receive -F tank/vm1"

#background copy is done we can rebuil the volume locally
dmsetup suspend /dev/mapper/c00-snap
echo "0 `blockdev --getsize /dev/sdc` snapshot-merge /dev/zvol/tank/vm1 /dev/zvol/tank/vm1-temp-write P 16" | dmsetup reload /dev/mapper/c00-snap
dmsetup resume /dev/mapper/c00-snap

#check then sync is finished
#done=`dmsetup status c00-snap |awk -F " |/" '{ if  ($4 == $6) { print "ok"} }'`
#if [ $done == "ok" ]; then
#  echo synced local storage
#fi

dmsetup suspend /dev/mapper/c00-snap
echo "0 `blockdev --getsize /dev/sdc` linear /dev/zvol/tank/vm1 0" | dmsetup reload /dev/mapper/c00-snap
dmsetup resume /dev/mapper/c00-snap

#cleanup
zfs destroy tank/vm1@test-final4
zfs destroy tank/vm1-temp-write
