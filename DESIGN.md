# Components

- Frontend: sends requests only to the NAS for managing the disks

- NAS: is responsible to manage the state of the disk images (is the disk
  available, is it mapped on some compute, etc.) and orchestrate their
  deployment on compute node

- Compute: can create mapping of a VM disk image under the request of a NAS

# Requests available on a NAS node

- set_zvol(zvol, hosting, size) => devicepath

    It maps the given `zvol` between the NAS itself and the `hosting` node.
    If the `zvol` does not exist it will be created with the given `size`

- tear_down(zvol)

    It removes the mapping currently in place for the given `zvol`. Mappings
    are persistent they survive reboot of the hosting node.
    The only way to remove a mapping is through this function.

- del_zvol(zvol)

    Erase the given `zvol` from the NAS.

- list_zvols() => list of zvols with their mappings

The current implementation uses zvol = vhostname
