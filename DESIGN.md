# Components

- Frontend: sends requests only to the NAS for managing the disks (except list_dev call to compute)

- NAS: is responsible to manage the state of the disk images (is the disk
  available, is it mapped on some compute, etc.) and orchestrate their
  deployment on compute node

- Compute: can create mapping of a VM disk image under the request of a NAS

# Requests available on a NAS node

- map_zvol(zvol, remotehost, size) => devicepath

    It maps the given `zvol` between the NAS itself and the `renotehost` node.
    If the `zvol` does not exist it will be created with the given `size`

- unmap_zvol(zvol)

    It removes the mapping currently in place for the given `zvol`. Mappings
    are persistent they survive reboot of the hosting node.
    The only way to remove a mapping is through this function.

- del_zvol(zvol)

    Erase the given `zvol` from the NAS.

- list_zvols(name) => list of zvols with their mappings

    Name of the zvol if empty return the full list.

The current implementation uses zvol = vhostname

# Requests available on a Compute node

- set_zvol(target, nas) => bdev

    Compute node connects the iSCSI `target` from `nas` to local `bdev` and returns one.

- tear_down(target)

    Compute node disconnects the target.
    
- list_dev()

    Compute node returns the list of connected iSCSI targets to the Frontend directly
    
![RabbitMQ messaging scheme](/rabbitmq_scheme.png?raw=true "Messaging scheme")

# Messages addressing

Frontend opens a new random-named queue for every command and blocks until a message comes to the queue, which contains the command response (or error). NAS and Compute nodes can send a response message to the '' (empty name) exchange with routing_key=random_queue_name which will be delivered to the waiting command's queue. The name of the queue is passed in reply_to attribute of the message by Frontend.

NAS and Compute nodes are exchanging messages using the rocks.vm-manage exchange, which redirects them to either NAS or Compute queues based on routing_key. All messages contain random "message_id" field to track them and get proper response if the message can't be delivered to the recepient. The return message has "correlation_id" attribute equals to the "message_id" of the requesting message.
