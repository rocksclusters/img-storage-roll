# IMG-STORAGE roll

img-storagee roll is a Rocks(r) roll to be used for managing virtual machines images on the NAS and Compute nodes

The roll creates the daemon scripts that can be started/stopped using system command 'service img-storage-{nas,vm}' start/stop/restart

Also the roll provides rocks commands for managing virtual machines. These include creating a zvol with mapping to a compute node local block device, unmapping and removing a zvol, getting a list of zvols created and zvols mapped on a compute node.

## Commands:

```rocks start host vm {vm_hostname}``` - mount the VM image and start the VM

```rocks stop host vm {vm_hostname}``` - stop the VM and unmount the VM image

```rocks add host storagemap {nas} {volume} {remote_host} {size}``` - create zvol if necessary and mount it to remote compute node

```rocks remove host storagemap {nas} {volume}``` - umount zvol from remote compute node

```rocks remove host storageimg {nas} {volume}``` - delete unmounted zvol from nas

```rocks list host storagemap {nas}``` - list NAS zvol bindings to iSCSI targets and compute nodes

```rocks list host storageimg {compute}``` - list compute node block devices mapped to NAS zvols

```rocks add cluster {fe_public_ip} {num_compute_nodes} cluster-naming=true fe-name={fe_name} container-hosts="{compute_host_0} {compute_host_1}"``` - create new virtual cluster
