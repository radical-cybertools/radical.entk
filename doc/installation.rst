.. _installation:

************
Installation
************

Installing Ensemble MD Toolkit
==============================

To install the EnsembleMD Toolkit Python modules in a virtual environment, 
open a terminal and run:

.. code-block:: bash

    virtualenv $HOME/EnMDToolkit
    source $HOME/EnMDToolkit/bin/activate
    pip install --upgrade git+https://github.com/radical-cybertools/radical.ensemblemd.git@master#egg=radical.ensemblemd

You can check the version of the installed EnsembleMD Toolkit like this:

.. code-block:: bash

    python -c "import radical.ensemblemd; print radical.ensemblemd.version"


.. _envpreparation:

Preparing the Environment
=========================

Ensemble MD Toolkit is a client-side library and relies on a set of external
software packages. One of these packages is `radical.pilot <http://radicalpilot.readthedocs.org>`_, 
an HPC cluster resource access and management library. It can access HPC clusters
remotely via SSH and GSISSH, but it requires (a) a MongoDB server and (b) a 
properly set-up SSH environment.

.. figure:: images/hosts_and_ports.*
   :width: 360pt
   :align: center
   :alt: MongoDB and SSH ports.

   `Figure 1: Typical Ensemble MD Toolkit environment set up.`

MongoDB Server
--------------

The MongoDB server is used to store and retrieve operational data during the 
execution of an Ensemble MD Toolkit application. The MongoDB server must 
be reachable on **port 27017** from **both**, the host that runs the 
Ensemble MD Toolkit application and the host that executes the MD tasks, i.e., 
the HPC cluster (see blue arrows in the figure above). In our experience,
a small VM instance (e.g., Amazon AWS) works exceptionally well for this.

.. warning:: If you want to run your application on your laptop or private 
             workstation, but run your MD tasks on a remote HPC cluster, 
             installing MongoDB on your laptop or workstation won't work.
             Your laptop or workstations usually does not have a public IP
             address and is hidden behind a masked and firewalled home or office 
             network. This means that the components running on the HPC cluster 
             will not be able to access the MongoDB server.

A MongoDB server can support more than one user. In an environment where 
multiple users use EnsembleMD toolkit applications, a single MongoDB server
for all users / hosts is usually sufficient. 

Install your own MongoDB
^^^^^^^^^^^^^^^^^^^^^^^^

Once you have identified a host that can serve as the new home for MongoDB,
installation is straight forward. You can either install the MongoDB 
server package that is provided by most Linux distributions, or 
follow the installation instructions on the MongoDB website:

http://docs.mongodb.org/manual/installation/

MongoDB-as-a-Service
^^^^^^^^^^^^^^^^^^^^

There are multiple commercial providers of hosted MongoDB services, some of them
offering free usage tiers. We have had some good experience with the following:

* https://mongolab.com/

HPC Cluster Access
------------------

In order to execute MD tasks on a remote HPC cluster, you need to set-up
password-less SSH login for that host. This can either be achieved via 
an ssh-agent that stores your SSH key's password (e.g., default on
OS X) or by setting up password-less SSH keys.

Password-less SSH with ssh-agent
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An ssh-agent asks you for your key's password the first time you use  it and
then stores it for you so that you don't have to enter it again. On OS X (>=
10.5), an ssh-agent is running by default. On other Linux operating systems
you might have to install or launch it manually.

You can test whether an ssh-agent is running by default on your system if you
log-in via SSH into the remote host twice. The first time, the ssh-agent 
should ask you for a password, the second time, it shouldn't. You can use the 
``ssh-add`` command to list all keys that are currently managed by your 
ssh-agent::

    %> ssh-add -l
    4096 c3:d6:4b:fb:ce:45:b7:f0:2e:05:b1:81:87:24:7f:3f /Users/enmdtk/.ssh/rsa_work (RSA)

For more information on this topic, please refer to this article:

* http://mah.everybody.org/docs/ssh

Password-less SSH keys
^^^^^^^^^^^^^^^^^^^^^^

.. warning:: Using password-less SSH keys is really not encouraged. Some sites might 
             even have a policy in place prohibiting the use of password-less
             SSH keys. Use ssh-agent if possible.

**These instructions were taken from http://www.linuxproblem.org/art_9.html**


Follow these instructions to create and set-up a public-private key pair that 
doesn't have a password.

As ``user_a`` on host ``workstation``, generate a pair of keys. 
Do not enter a passphrase::

    user_a@workstation:~> ssh-keygen -t rsa

    Generating public/private rsa key pair.
    Enter file in which to save the key (/home/a/.ssh/id_rsa): 
    Created directory '/home/a/.ssh'.
    Enter passphrase (empty for no passphrase): 
    Enter same passphrase again: 
    Your identification has been saved in /home/a/.ssh/id_rsa.
    Your public key has been saved in /home/a/.ssh/id_rsa.pub.
    The key fingerprint is:
    3e:4f:05:79:3a:9f:96:7c:3b:ad:e9:58:37:bc:37:e4 a@A

Now use ssh to create a directory ~/.ssh as ``user_b`` on ``cluster``. 
(The directory may already exist, which is fine)::

    user_a@workstation:~> ssh user_b@cluster mkdir -p .ssh
    user_b@cluster's password: 

Finally append ``usera_a``'s new public key to ``user_b@cluster:.ssh/authorized_keys`` 
and enter ``user_b``'s password one last time::

    user_a@workstation:~> cat .ssh/id_rsa.pub | ssh user_b@cluster 'cat >> .ssh/authorized_keys'
    user_b@cluster's password: 

From now on you can log into ``cluster`` as ``user_b`` from ``workstation`` as 
``user_a`` without a password::

    user_a@workstation:~> ssh user_b@cluster

.. note:: Depending on your version of SSH you might also have to do the following changes:

            - Put the public key in ``.ssh/authorized_keys2`` (note the **2**)
            - Change the permissions of .ssh to 700
            - Change the permissions of .ssh/authorized_keys2 to 640

