#!/bin/bash
 
if [ $# -lt 1 ] ; then
   echo "Usage: $0 zpool zvol remotehost remotehost_zpool throttle is_delete_remote
f.e.: $0 tank vm-hpcdev-pub03-0-vol compute-0-1 tank 10G 0"
   exit 1
fi

REMOTE_SNAPSHOTS_TRIM=3
LOCAL_SNAPSHOTS_TRIM=3

ZPOOL=$1
ZVOL=$2
REMOTEHOST=$3
REMOTEZPOOL=$4
THROTTLE=$5
IS_DELETE_REMOTE=$6

SNAP_NAME=`/usr/bin/uuidgen`
LOCAL_LAST_SNAP_NAME=`/sbin/zfs list -Hpr -t snapshot -o name -s creation "$ZPOOL/$ZVOL" | tail -n 1 | sed -e 's/.\+@//g'`

/bin/su img-storage -c "/usr/bin/ssh $REMOTEHOST \"/sbin/zfs snap $REMOTEZPOOL/$ZVOL@$SNAP_NAME\""
if [ -n "$THROTTLE" ]; then
    /bin/su img-storage -c "/usr/bin/ssh $REMOTEHOST \"/sbin/zfs send -i $REMOTEZPOOL/$ZVOL@$LOCAL_LAST_SNAP_NAME $REMOTEZPOOL/$ZVOL@$SNAP_NAME | pv -L $THROTTLE -q \"" | /sbin/zfs receive -F "$ZPOOL/$ZVOL"
else
    /bin/su img-storage -c "/usr/bin/ssh $REMOTEHOST \"/sbin/zfs send -i $REMOTEZPOOL/$ZVOL@$LOCAL_LAST_SNAP_NAME $REMOTEZPOOL/$ZVOL@$SNAP_NAME                      \"" | /sbin/zfs receive -F "$ZPOOL/$ZVOL"
fi

#trim remote snapshots
if (( $IS_DELETE_REMOTE )); then
    /bin/su img-storage -c "/usr/bin/ssh $REMOTEHOST \"/sbin/zfs destroy -r $REMOTEZPOOL/$ZVOL\""
else
    /bin/su img-storage -c "/usr/bin/ssh $REMOTEHOST \"/sbin/zfs list -Hpr -t snapshot -o name -s creation $REMOTEZPOOL/$ZVOL | head -n -$REMOTE_SNAPSHOTS_TRIM | xargs -r -l1 /sbin/zfs destroy\""
fi

#trim local snapshots
/sbin/zfs list -Hpr -t snapshot -o name -s creation "$ZPOOL/$ZVOL" | head -n "-$LOCAL_SNAPSHOTS_TRIM" | xargs -r -l1 /sbin/zfs destroy
