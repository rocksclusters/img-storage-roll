#!/bin/bash
 
if [ $# -lt 1 ] ; then
   echo "Usage: $0 zpool zvol remotehost is_delete_remote
f.e.: $0 tank vm-hpcdev-pub03-0-vol compute-0-1 0"
   exit 1
fi

# Save standard output and standard error
exec 3>&1 4>&2
# Redirect standard output to a log file
exec 1>/tmp/stdout.log
# Redirect standard error to a log file
exec 2>/tmp/stderr.log

PREFIX="IMG-STORAGE-"

REMOTE_SNAPSHOTS_TRIM=10
LOCAL_SNAPSHOTS_TRIM=10

ZPOOL=$1
ZVOL=$2
REMOTEHOST=$3
IS_DELETE_REMOTE=$4

REMOTEZPOOL=`/opt/rocks/bin/rocks list host attr $REMOTEHOST | grep "vm_container_zpool " | awk '{print $3}'`
THROTTLE=`/opt/rocks/bin/rocks list host attr $REMOTEHOST | grep "img_download_speed " | awk '{print $3}'`

SNAP_NAME=$PREFIX+`/usr/bin/uuidgen`
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
    /bin/su img-storage -c "/usr/bin/ssh $REMOTEHOST \"/sbin/zfs list -Hpr -t snapshot -o name -s creation $REMOTEZPOOL/$ZVOL | grep $PREFIX | head -n -$REMOTE_SNAPSHOTS_TRIM | xargs -r -l1 /sbin/zfs destroy\""
fi

#trim local snapshots
/sbin/zfs list -Hpr -t snapshot -o name -s creation "$ZPOOL/$ZVOL" | head -n "-$LOCAL_SNAPSHOTS_TRIM" | xargs -r -l1 /sbin/zfs destroy
