#!/bin/bash
 
if [ $# -lt 1 ] ; then
   echo "Usage: $0 zpool zvol remotehost remotezpool is_delete_remote [throttle]
f.e.: $0 tank vm-hpcdev-pub03-0-vol compute-0-1 tank 0 10m"
   exit 1
fi

# Save standard output and standard error
exec 3>&1 4>&2
# Redirect standard output to a log file
exec 1>/tmp/stdout.log
# Redirect standard error to a log file
exec 2>/tmp/stderr.log

set -e

PREFIX="IMG-STORAGE-"

REMOTE_SNAPSHOTS_TRIM=10
LOCAL_SNAPSHOTS_TRIM=10

ZPOOL=$1
ZVOL=$2
REMOTEHOST=$3
REMOTEZPOOL=$4
IS_DELETE_REMOTE=$5
THROTTLE=$6

SNAP_NAME=$PREFIX`/usr/bin/uuidgen`
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
