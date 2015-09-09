#!/bin/bash
 
if [ $# -lt 1 ] ; then
   echo "Usage: $0 zpool zvol remotehost
f.e.: $0 tank vm-hpcdev-pub03-0-vol compute-0-1"
   exit 1
fi

# Save standard output and standard error
exec 3>&1 4>&2
# Redirect standard output to a log file
exec 1>/tmp/stdout.log
# Redirect standard error to a log file
exec 2>/tmp/stderr.log


LOCAL_SNAPSHOTS_TRIM=10

ZPOOL=$1
ZVOL=$2
REMOTEHOST=$3

REMOTEZPOOL=`/opt/rocks/bin/rocks list host attr $REMOTEHOST | grep "vm_container_zpool " | awk '{print $3}'`
THROTTLE=`/opt/rocks/bin/rocks list host attr $REMOTEHOST | grep "img_upload_speed " | awk '{print $3}'`

SNAP_NAME=`/usr/bin/uuidgen`

/sbin/zfs snap "$ZPOOL/$ZVOL@$SNAP_NAME"

if [ -n "$THROTTLE" ]; then
    /sbin/zfs send "$ZPOOL/$ZVOL@$SNAP_NAME" | pv -L "$THROTTLE" -q | su img-storage -c "ssh $REMOTEHOST \"/sbin/zfs receive -F $REMOTEZPOOL/$ZVOL\""
else
    /sbin/zfs send "$ZPOOL/$ZVOL@$SNAP_NAME" |                      su img-storage -c "ssh $REMOTEHOST \"/sbin/zfs receive -F $REMOTEZPOOL/$ZVOL\""
fi

#trim local snapshots
/sbin/zfs list -Hpr -t snapshot -o name -s creation "$ZPOOL/$ZVOL" | head -n "-$LOCAL_SNAPSHOTS_TRIM" | xargs -r -l1 /sbin/zfs destroy

