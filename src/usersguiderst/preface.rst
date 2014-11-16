Preface
=======

The Img-storage Roll provides advanced virtaul machine disks management.
When installed on a Rocks Cluster along with the KVM Roll, and the
ZFS-Linux Roll, it provide a unified management system to store all
virtual disk on a NAS appliance and serve them through iSCSI to the
various VM Container. If properly configured it can also replicate
disk images localy to VM container in order to provide better
performances to the virtual machine and to off-load the central NAS
server.

Please read the KVM Roll documentation and the ZFS-Linux Roll
documentaion before proceeding.

Requirements
------------

This Roll requires the KVM Roll, and the zfs-linux Roll.
