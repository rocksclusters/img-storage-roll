#!/bin/bash
 
if [ $# -lt 1 ] ; then
   echo "Usage: $0 zpool zvol remotehost remotehost_zpool throttle
f.e.: $0 tank vm-hpcdev-pub03-0-vol compute-0-1 tank 10G"
   exit 1
fi

LOCAL_SNAPSHOTS_TRIM=3

ZPOOL=$1
ZVOL=$2
REMOTEHOST=$3
REMOTEZPOOL=$4
THROTTLE=$5

SNAP_NAME=`/usr/bin/uuidgen`

/sbin/zfs snap "$ZPOOL/$ZVOL@$SNAP_NAME"

if [ -n "$THROTTLE" ]; then
    /sbin/zfs send "$ZPOOL/$ZVOL@$SNAP_NAME" | pv -L "$THROTTLE" -q | su img-storage -c "ssh $REMOTEHOST \"/sbin/zfs receive -F $REMOTEZPOOL/$ZVOL\""
else
    /sbin/zfs send "$ZPOOL/$ZVOL@$SNAP_NAME" |                      su img-storage -c "ssh $REMOTEHOST \"/sbin/zfs receive -F $REMOTEZPOOL/$ZVOL\""
fi

#trim local snapshots
/sbin/zfs list -Hpr -t snapshot -o name -s creation "$ZPOOL/$ZVOL" | head -n "-$LOCAL_SNAPSHOTS_TRIM" | xargs -r -l1 /sbin/zfs destroy

