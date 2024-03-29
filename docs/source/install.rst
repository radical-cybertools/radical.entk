.. _installation:

************
Installation
************

Installing Ensemble Toolkit
===========================

Installing Ensemble Toolkit using virtualenv
----------------------------------------------

To install the Ensemble Toolkit, we need to create a virtual environment.
Open a terminal and run:

.. code-block:: bash

        virtualenv $HOME/ve-entk -p python3.7

- ``-p`` params indicates which python version you use, python3.7+ is required

Activate virtualenv by:

.. code-block:: bash

        source $HOME/ve-entk/bin/activate

.. note:: Activated env name is indicated in the prompt like: ``(ve-entk) username@two:~$``

Deactivate the virtualenv, if you want to disengage. Your python won't
recognize EnTK after deactivation.  To deactivate, run:

.. code-block:: bash

        deactivate

It is suggested to use the released version of EnTK which you can install
by executing the following command in your virtualenv:

.. code-block:: bash

        pip install radical.entk


To install a specific branch of EnTK, e.g., `devel` instead of using pip
installation, you will need to clone the repository and checkout the branch.
You can do so using the following commands:

.. code-block:: bash

        git clone https://github.com/radical-cybertools/radical.entk.git
        cd radical.entk
        git checkout <branch-name>
        pip install .



You can check the version of Ensemble Toolkit with the
```radical-stack``` command-line tool. The currently installed version should
be printed.

.. code-block:: bash

        radical-stack

          python               : 3.7.4
          pythonpath           :
          virtualenv           : /home/username/ve-entk

          radical.entk         : 1.0.0
          radical.pilot        : 1.0.0
          radical.saga         : 1.0.0
          radical.utils        : 1.0.0


Installing Ensemble Toolkit using Anaconda/Conda
------------------------------------------------

Conda users can obtain Ensemble Toolkit from `conda-forge` channel.
To install the Ensemble Toolkit, we need to create a conda environment.
Open a terminal and run (assuming you have PATH to point to ``conda``):

.. code-block:: bash

        conda create -n conda-entk python=3.7 -c conda-forge -y
        conda activate conda-entk


It is suggested to use the released version of EnTK which you can install
by executing the following command in your conda env:

.. code-block:: bash

        conda install radical.entk -c conda-forge


You can check the version of Ensemble Toolkit with the
```radical-stack``` command-line tool. The currently installed version should
be printed.

.. code-block:: bash

        radical-stack

          python               : <path>/rct/bin/python3
          pythonpath           :
          version              : 3.9.2
          virtualenv           : rct
          radical.entk         : 1.6.0
          radical.pilot        : 1.6.3
          radical.saga         : 1.6.1
          radical.utils        : 1.6.2



Preparing the Environment
=========================

Ensemble Toolkit uses `RADICAL Pilot <http://radicalpilot.readthedocs.org>`_ as
the runtime system. RADICAL Pilot can access HPC clusters remotely via SSH but
it requires: (1) a MongoDB server; and (2) a properly set-up passwordless SSH
environment.

.. _ssh_gsissh_setup:

Setup passwordless SSH Access to HPC platforms
----------------------------------------------

In order to create a passwordless access to another machine, you need to create
a key-pair on your local machine and paste the public key into the
`authorizes_users` list on the remote machine.

`This <http://linuxproblem.org/art_9.html>`_ is a recommended tutorial to create
password ssh access.

An easy way to setup SSH access to multiple remote machines is to create a file
``~/.ssh/config``. Suppose the URL used to access a specific machine is
``foo@machine.example.com``. You can create an entry in this config file as
follows:

.. code-block:: bash

        # contents of $HOME/.ssh/config
        Host machine1
                HostName machine.example.com
                User foo

Now you can login to the machine by ``ssh machine1``.


Source: http://nerderati.com/2011/03/17/simplify-your-life-with-an-ssh-config-file/


.. _troubleshooting:

Troubleshooting
=======================

**Missing virtualenv**

This should return the version of the RCT installation, e.g., `1.0.0`.

If virtualenv **is not** installed on your system, you can try the following.

.. code-block:: bash

        wget --no-check-certificate https://pypi.python.org/packages/source/v/virtualenv/virtualenv-16.7.9.tar.gz
        tar xzf virtualenv-16.7.9.tar.gz

        python virtualenv-16.7.9/virtualenv.py $HOME/ve-entk -p python3.7
        source $HOME/ve-entk/bin/activate
