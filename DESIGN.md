# Components

- Frontend: sends commands only to the NAS for managing the disks
- NAS: is responsible to keep the state of the disk images (on which compute node
  they are currently mapped) and orchestrate their deployment on compute node
  FE send only order to the NAS
- Compute: can create mapping a a VM image under the request of a NAS

# current requests

- NAS: 
  - setup_zvol (zvol, compute_name) 
  - teardown_zvol (zvol, compute_name)

- compute (or vm-container):
  - set_zvol (zvol, nas_name, iscsi_target)
  - tear_down (zvol, nas_name, iscsi_target)




# future requests (design attempt)

- NAS (nasname_zvol is a unique name space):
  - setup_zvol (zvol, compute_name, size) => (devicepath) || (error_msg)
    what if size is different than original creation size?
  - teardown_zvol (zvol) => (error_msg)
  - remove_zvol (zvol) => (error_msg)
    the user has first to call tear_down if the volume is mapped
  - query_zvol (zvol) => (compute_name, size, devicepath) || (unmapped) || (donotexist)
  - list_zvol () => (zvol, zvol, zvol, ...)
    is this really necessary?
  - create_zvol for the moment is implicit in setup_zvol if it is not already present



