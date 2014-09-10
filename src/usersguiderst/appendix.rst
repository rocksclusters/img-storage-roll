========
Appendix
========

Attributes table
================

+-----------------------+------------------------------------------------------+
|Attribute Name         |Description of its function                           |
+=======================+======================================================+
|``img_storage_vm``     |It enables installation of the client side disk       |
|                       |management system (by default all vm-container        |
|                       |appliance)                                            |
+-----------------------+------------------------------------------------------+
|``img_storage_nas``    |It enables installation of the server side disk       |
|                       |management system (by default all NAS appliance)      |
+-----------------------+------------------------------------------------------+
|``IB_net``             |It can be used to specify the network interface used  |
|                       |to send the IO data                                   |
+-----------------------+------------------------------------------------------+
|``img_zpools``         |It can be used to specify a default zpool to allocate |
|                       |VM disk images on the NAS. It will be used by the     |
|                       |``rocks set host vm nas`` to set the zpool parameter  |
+-----------------------+------------------------------------------------------+
|``img_sync``           |If equal to true it will enable local synchronized    |
|                       |disk on the given vm container (default unset)        |
+-----------------------+------------------------------------------------------+
|``vm_container_zpool`` |If img_sync is enable this attribute specifies the    |
|                       |zpool name that will be used to store temporarly VM   |
|                       |disk images on the vm-container                       |
+-----------------------+------------------------------------------------------+
|``img_part_zfs_mirror``|If equal to true it enables standard partitioning on  |
|                       |nodes where img_storage_vm is enabled.                |
+-----------------------+------------------------------------------------------+



ROCKS Copyright
===============

.. literalinclude:: rocks-copyright.txt


Third Party Copyrights and Licenses
===================================

This section enumerates the licenses from all the third party software
components of this Roll. A "best effort" attempt has been made to insure the
complete and current licenses are listed. In the case of errors or ommisions
please contact the maintainer of this Roll. For more information on the
licenses of any components please consult with the original author(s) or see
the Rocks® `GIT repository <http://github.com/rocksclusters>`_.




RabbitMQ
--------

::


         The contents of this file are subject to the Mozilla Public License
         Version 1.1 (the "License"); you may not use this file except in
         compliance with the License. You may obtain a copy of the License at
         http://www.mozilla.org/MPL/

         Software distributed under the License is distributed on an "AS IS"
         basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
         License for the specific language governing rights and limitations
         under the License.

         The Original Code is RabbitMQ.

         The Initial Developer of the Original Code is GoPivotal, Ltd.
         Copyright (c) 2007-2013 GoPivotal, Inc.  All Rights Reserved.

Pika
----

::


         The contents of this file are subject to the Mozilla Public License
         Version 1.1 (the "License"); you may not use this file except in
         compliance with the License. You may obtain a copy of the License at
         http://www.mozilla.org/MPL/

         Software distributed under the License is distributed on an "AS IS"
         basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
         License for the specific language governing rights and limitations
         under the License.

         The Original Code is Pika.

         The Initial Developers of the Original Code are VMWare, Inc. and
         Tony Garnock-Jones.

         Portions created by VMware, Inc. or by Tony Garnock-Jones are
         Copyright (C) 2009-2011 VMware, Inc. and Tony Garnock-Jones.

