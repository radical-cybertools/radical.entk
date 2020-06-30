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

- ``-p`` params indicates which python version you use, python3.6+ is required
- A legacy python2 installation is available with the ``0.72.1`` version. Hot fixes will be provided until Jul 2020. Read more at
  the troubleshooting_

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

To install the Ensemble Toolkit, we need to create a conda environment. 
Open a terminal and run (assuming you have PATH to point to ``conda``):

.. code-block:: bash

        conda create -n conda-entk python=3.7 -y
        conda activate conda-entk


It is suggested to use the released version of EnTK which you can install
by executing the following command in your conda env:

.. code-block:: bash

        conda install radical.entk


You can check the version of Ensemble Toolkit with the
```radical-stack``` command-line tool. The currently installed version should
be printed.

.. code-block:: bash

        radical-stack

          python               : 3.6.9
          pythonpath           :
          virtualenv           : /home/hrlee/venv3/entk.py36

          radical.entk         : 1.0.0
          radical.pilot        : 1.0.0
          radical.saga         : 1.0.0
          radical.utils        : 1.0.0



.. comments

        Installing Ensemble Toolkit using Docker
        ----------------------------------------

        You can install Docker from their 
        `official documentation <https://hub.docker.com/search/?type=edition&offering=community>`_.
        Once you have installed Docker, you can use the following Dockerfile to build
        a container:

        .. code-block:: bash

                FROM ubuntu:16.04

                ENV RMQ_HOSTNAME=two.radical-project.org
                ENV RMQ_PORT=33247
                ENV RADICAL_PILOT_DBURL="mongodb://user:user@ds247688.mlab.com:47688/entk-docs"

                RUN apt-get update \
                && apt-get install wget curl python python-dev python-pip python-virtualenv bzip2 -y \
                && virtualenv ~/ve-entk \
                && . ~/ve-entk/bin/activate \
                && pip install radical.entk

        You can also download the Dockerfile :download:`here <./misc/Dockerfile>`.

        You can build and execute the container by running:

        .. code-block:: bash

                docker build -f ./Dockerfile -t entk .
                docker run -t -i entk

        Once you execute the container, the default path will be /root (of the container).
        The EnTK virtualenv exists at ~/ve-entk (inside the container). This is useful
        to know as the examples exist inside the virtualenv.

        You can check the version of Ensemble Toolkit with the
        ```radical-entk-version``` command-line tool. The current version should be
        printed.

        .. code-block:: bash

                radical-entk-version
                0.70.0


RabbitMQ
========

Ensemble Toolkit relies on RabbitMQ for message transfers. RabbitMQ needs to be
configured or it can be installed on the same machine as EnTK is installed.
Installation instructions can be found at
<https://www.rabbitmq.com/download.html>. At the end of the installation run
```rabbitmq-server``` to start the server.

The following configuration defines a default server and port number to
communicate. Note that RMQ username and password might be required to 
complete authentication.

.. code-block:: bash

        export RMQ_HOSTNAME={IP ADDRESS};
        export RMQ_PORT={PORT NUMBER};
        export RMQ_USERNAME={USERNAME};
        export RMQ_PASSWORD={PASSWORD};

.. note:: {} sections need to be replaced with actual values, and EnTK
        administrators are able to provide these information.

RMQ Account
-----------

Open a new ticket asking a new RMQ account:
https://github.com/radical-cybertools/radical.entk/issues

.. comments

        Installing rabbitmq
        ===================

        Installing rabbitmq as a system process (sudo privileges required)
        ------------------------------------------------------------------

        Ensemble Toolkit relies on RabbitMQ for message transfers. Installation
        instructions can be found at <https://www.rabbitmq.com/download.html>. At
        the end of the installation run ```rabbitmq-server``` to start the server.
        RabbitMQ needs to be installed on the same machine as EnTK is installed.

        In some cases, you might have to explicitly start the rabbitmq-server after
        installation. You can check if the rabbitmq-server process is alive. If not,
        please run the following:

        .. code-block:: bash

                rabbitmq-server -detached


        Installing rabbitmq using docker
        --------------------------------

        If installing rabbitmq directly seems to be cumbersome, you can also install a
        docker instance of rabbitmq. Assuming you have docker installed, you can
        download and run the rabbitmq instance using the following command:

        .. code-block:: bash

                docker run -d --name <name of instance> -P rabbitmq:3


        The '-P' argument auto maps new ports from localhost to the ports expected by
        rabbitmq. This is useful if you want to have multiple EnTK scripts running as
        you would require multiple rabbitmq instances.

        You can see the mapping of the ports running ```docker ps```.

        .. code-block:: bash

                vivek@two:~$ docker run -d --name rabbit-1 -P rabbitmq:3
                fb8ee8bfd822656a6338b7c19fa6a9641944f8bf5de5c1414fb78d049fdffc42
                vivek@two:~$ docker ps
                CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                                                                                                 NAMES
                fb8ee8bfd822        rabbitmq:3          "docker-entrypoint..."   9 seconds ago       Up 7 seconds        0.0.0.0:32777->4369/tcp, 0.0.0.0:32776->5671/tcp, 0.0.0.0:32775->5672/tcp, 0.0.0.0:32774->25672/tcp   rabbit-1


        Interactions between RabbitMQ and EnTK are done through port 5672 by default.
        For the above docker instance, we need to use port 32775. In your EnTK scripts,
        while creating the AppManager, you need to specify port=32775.

        .. note::
           If you are using Docker to install both EnTK and RabbitMQ, they should run
           as two different containers. You can set the RMQ_PORT in the EnTK container
           accordingly.

        Installation Video
        ==================

        .. raw:: html

                <video controls width="800" src="_static/entk_installation_get_started.mp4"></video>


Preparing the Environment
=========================

Ensemble Toolkit uses `RADICAL Pilot <http://radicalpilot.readthedocs.org>`_ as
the runtime system. RADICAL Pilot can access HPC clusters remotely via SSH and
GSISSH, but it requires (a) a MongoDB server and (b) a properly set-up
passwordless SSH/GSISSH environment.


.. comments

        MongoDB Server
        --------------

        .. figure:: figures/hosts_and_ports.png
             :width: 360pt
             :align: center
             :alt: MongoDB and SSH ports.

        The MongoDB server is used to store and retrieve operational data during the
        execution of an application using RADICAL-Pilot. The MongoDB server must
        be reachable on **port 27017** from **both**, the host that runs the
        Ensemble Toolkit application and the host that executes the MD tasks, i.e.,
        the HPC cluster (see blue arrows in the figure above). In our experience,
        a small VM instance (e.g., Amazon AWS) works exceptionally well for this.

        .. warning:: If you want to run your application on your laptop or private
                    workstation, but run your MD tasks on a remote HPC cluster,
                    installing MongoDB on your laptop or workstation won't work.
                    Your laptop or workstation usually does not have a public IP
                    address and is hidden behind a masked and firewalled home or office
                    network. This means that the components running on the HPC cluster
                    will not be able to access the MongoDB server.

        A MongoDB server can support more than one user. In an environment where
        multiple users use Ensemble Toolkit, a single MongoDB server
        for all users / hosts is usually sufficient.

        **Install your own MongoDB**

        Once you have identified a host that can serve as the new home for MongoDB,
        installation is straight forward. You can either install the MongoDB
        server package that is provided by most Linux distributions, or
        follow the installation instructions on the MongoDB website:

        http://docs.mongodb.org/manual/installation/

        **MongoDB-as-a-Service**

        There are multiple commercial providers of hosted MongoDB services, some of them
        offer free usage tiers. We have had some good experience with the following:

        * https://mongolab.com/


.. _ssh_gsissh_setup:

Setup passwordless SSH Access to HPC resources
----------------------------------------------

In order to create a passwordless access to another machine, you need to create a RSA key on your local machine
and paste the public key into the `authorizes_users` list on the remote machine.

`This <http://linuxproblem.org/art_9.html>`_ is a recommended tutorial to create password ssh access.

An easy way to setup SSH access to multiple remote machines is to create a file ``~/.ssh/config``.
Suppose the url used to access a specific machine is ``foo@machine.example.com``. You can create an entry in this
config file as follows:

.. code-block:: bash

        # contents of $HOME/.ssh/config
        Host machine1
                HostName machine.example.com
                User foo

Now you can login to the machine by ``ssh machine1``.


Source: http://nerderati.com/2011/03/17/simplify-your-life-with-an-ssh-config-file/


Setup GSISSH Access to HPC resources
------------------------------------

Setting up GSISSH access to a machine is a bit more complicated. We have documented the steps to setup GSISSH on
`Ubuntu <https://github.com/vivek-bala/docs/blob/master/misc/gsissh_setup_stampede_ubuntu_xenial.sh>`_ (tested for
trusty and xenial) and `Mac <https://github.com/vivek-bala/docs/blob/master/misc/gsissh_setup_mac>`_. Simply execute
all the commands, see comments for details.

The above links document the overall procedure and how to get certificates to access XSEDE machines. Depending on the machine
you want to access, you will have to get the certificates from the corresponding locations. In most cases, this
information is available in their user guide.


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

**Python 2 legacy installation**

As of January 1, 2020, Python 2 support is terminated by the Python Software
Foundation but the previous release of EnTK i.e. ``0.72.1`` allows to use Python 2.7.
PyPI installation with virtualenv is:

.. code-block:: bash

        virtualenv $HOME/ve-entk-py2 -p python2.7
        source $HOME/ve-entk-py2/bin/activate
        pip install radical.entk==0.72.1

```radical-stack``` confirms the versions of the radical cybertools:

.. code-block:: bash

        $ radical-stack

          python               : 2.7.17
          pythonpath           :
          virtualenv           : /home/username/ve-entk-py2

          radical.entk         : 0.72.1
          radical.pilot        : 0.73.1
          radical.saga         : 0.72.1
          radical.utils        : 0.72.0

