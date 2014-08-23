==========================
Using the Img-storage Roll
==========================

To use the Img-Storage roll is necessary to have at least one NAS server
which can store all the virtual disk images. Moreover it is required to
install the full OS roll.

Overview of the Img-Storage server
==================================

The Img-storage roll needs several daemons to properly function. Each
daemon is implemented as an init script under ``/etc/init.d`` and can be
restared by the service command. The tree main services are:

1. *rabbitmq-server* this component is used to orchestrate all
   comunication between the various nodes and is installed on the
   frontend. It is just a standard ``Rabbit MQ Server``.

2. *img-storage-vm* this is the python daemon which is in charge of
   managing the iSCSI mapping on the hosting machine (the machine that
   will run the virtual machine). img-storage-vm is installed by default
   on all VM Container appliances but it can be installed on other nodes
   simply by turning to true the attribute ``img_storage_vm``. You will
   also need KVM component on the node to properly run virtual machine,
   so also the attribute ``
   kvm`` should be set to true.

3. *img-storage-nas* this is the python daemon which is in manages the
   virtual disk repository. It uses ZFS as the underlying technology for
   storage. This daemon is also responsible to set up iSCSI targets for
   the img-storage-vm. img-storage-vm is installed by default on all
   NASes appliance but it can be changed simply using the attribute
   ``img_storage_vm`` (the attribute ``zfs`` should also be true in
   order to install zfs). img-storage-nas allocates virtual disks on a
   zpool called tank, which should be created manually by the
   administrator before any virtual machine can be used.

For example to run virtual machine on a standard compute node it is
necessary to set:

::

    /opt/rocks/bin/rocks add appliance attr compute img_storage_vm true
    /opt/rocks/bin/rocks add appliance attr compute kvm true

Then reinstall all the compute nodes.

Enable remote vritual disk with Img-Storage
===========================================

To enable a virtual machine to use a remote virtual disk, the name of
the NAS holding the disk image must be set up. The command ``rocks set
host vm nas`` can be used for this, while the command ``rocks
list host vm nas`` can be used to see the current value of the NAS name.
If the NAS name is not specified for a virtual host, it will use the its
original disks configuration which by default uses local raw files
(``rocks list host vm showdisks=1``).

Once the NAS name is configured for a Virtual host, the virtual host
will use a remote iSCSI disk provided by the given NAS. For example if
we have a vritual compute node we can assign its NAS name with the
following commands:

::

    # rocks add host vm vm-container-0-14 compute
    added VM compute-0-14-0 on physical node vm-container-0-14
    # rocks set host vm nas compute-0-14-0 nas=nas-0-0
    # rocks start host vm compute-0-14-0
    nas-0-0:compute-0-14-0-vol mapped to vm-container-0-14:/dev/sdc

When the host is stopped (with ``rocks stop host vm``) the iSCSI mapping
will be removed automatically from the physical container. There are
also 'manual' commands to list, create or remove iSCSI mapping, as shown
below:

::

    # rocks list host storagemap nas-0-0
    DEVICE                                        HOST                 ZVOL                 
    --------------------------------------------- -------------------- compute-0-0-0-vol    
    iqn.2001-04.com.nas-0-0-compute-0-14-0-vol    vm-container-0-14    compute-0-14-0-vol  
    # rocks remove host storagemap nas-0-0 compute-0-14-0-vol
    # rocks list host storagemap nas-0-0
    DEVICE HOST ZVOL                 
    ------ ---- compute-0-0-0-vol    
    ------ ---- compute-0-14-0-vol   

The virtual disks are saved on the NAS specified in the ``rocks
list host vm nas`` under a zpool called ``tank``. Each volume is created
appending '-vol' to the virtual machine name.

::

    # ssh nas-0-0
    # zfs list
    NAME                      USED  AVAIL  REFER  MOUNTPOINT
    tank                      231G  1.61T  8.08G  /tank
    tank/compute-0-0-0-vol   37.1G  1.64T  3.60G  -
    tank/compute-0-14-0-vol  37.1G  1.64T  3.84G  -

