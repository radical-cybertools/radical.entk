.. _installation:

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
software packages. One of these packages is `radical.pilot <radicalpilot.readthedocs.org>`_, 
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
be reachable on **port 27017** from **both** hosts, the host that runs the 
Ensemble MD Toolkit application and the host that executes the workload, i.e., 
the HPC cluster (see blue arrows in the figure above).

.. warning:: If you run your application on your laptop or private workstation,
             but run your workload on a remote HPC cluster, installing MongoDB 
             on the same machine won't work: while the locally running Ensemble 
             MD Toolkit application will be able to connect to it, the 
             components on the remote HPC cluster won't. 

**Install your own MongoDB**

Once you have identified a host that can serve as the new home for MongoDB,
installation is straight forward. You can either just install the MongoDB 
server package that is provided by most Linux / Unix operating systems, or 
follow the installation instructions on the MongoDB website:

http://docs.mongodb.org/manual/installation/

**MongoDB-as-a-Service**

There are multiple commercial providers of hosted MongoDB services, some of them
even offering free usage tiers. We have some good experience with the following:

* https://mongolab.com/

HPC Cluster Access
------------------

todo