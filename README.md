# SDSC "img-storage" roll

## Overview

This roll bundles the packages for managing VM images in ROCKS cluster.

It includes:
- <a href="http://www.rabbitmq.com/" target="_blank">http://www.rabbitmq.com/</a> is a messaging broker implementing AMQP protocol and providing message exchange between cluster components.
- <a href="https://github.com/pika/pika" target="_blank">Pika</a> is a library for communication with RabbitMQ from Python.
- pythondaemon, lockfile - should be provided by base roll
- rocks-command-imagestorage - set of rocks commands
- img-storage-nas - the NAS daemon
- img-storage-vm - the Compute node daemon
- img-storage - the common library containing the code for all daemons


## Requirements

To build/install this roll you must have root access to a Rocks development
machine (e.g., a frontend or development appliance).

If your Rocks development machine does *not* have Internet access you must
download the appropriate img-storage source file(s) using a machine that does
have Internet access and copy them into the `src/<package>` directories on your
Rocks development machine.

This roll requires the full OS roll installed on the machine.
KVM roll and zfs-linux are also required.


## Building

To build the img-storage-roll, execute these instructions on a Rocks development
machine (e.g., a frontend or development appliance):

```shell
% make default 2>&1 | tee build.log
% grep "RPM build error" build.log
```

If nothing is returned from the grep command then the roll should have been
created as... `img-storage-*.iso`. If you built the roll on a Rocks frontend then
proceed to the installation step. If you built the roll on a Rocks development
appliance you need to copy the roll to your Rocks frontend before continuing
with installation.

## Installation

To install, execute these instructions on a Rocks frontend:

```shell
% rocks add roll *.iso
% rocks enable roll img-storage
% cd /export/rocks/install
% rocks create distro
% rocks run roll img-storage | bash
```
