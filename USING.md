# IMG-STORAGE roll

img-storagee roll is a Rocks(r) roll to be used for managing virtual machines images on the NAS and Compute nodes

The roll creates the daemon scripts that can be started/stopped using system command 'service img-storage-{nas,vm}' start/stop/restart

Also the roll provides rocks commands for managing virtual machines. These include creating a zvol with mapping to a compute node local block device, unmapping and removing a zvol, getting a list of zvols created and zvols mapped on a compute node.
