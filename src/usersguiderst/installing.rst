Installing
==========

On a New Server
---------------

The |roll-name| Roll can be installed during the initial
installation of your server (or cluster). This procedure is documented
in section 1.2 of the ROCKS usersguide. You should select the
|roll-name| from the list of available ROLLs when you see a
screen that is similar to the one below.

.. image:: image/roll-select-rolls.png


On an Existing Server
---------------------

The |roll-name| Roll may also be added onto an existing server (or
frontend). For sake of discussion, assume that you have an iso image of
the roll called |roll-name|.iso. The following procedure will
install the Roll, and after the server reboots the Roll should be fully
installed and configured.

.. parsed-literal::

    $ su - root
    # rocks add roll |roll-name|.iso
    # rocks enable roll |roll-name|
    # cd /export/rocks/install
    # rocks create distro
    # rocks run roll |roll-name| | bash
    # init 6

