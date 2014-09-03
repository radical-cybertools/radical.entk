.. radical.ensemblemd documentation master file, created by
   sphinx-quickstart on Thu Jul 24 13:51:41 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

radical.ensemblemd
##################

radical.ensemblemd is a Python API and library for developing and executing 
large-scale ensemble-based Molecular Dynamics simulations and workflows

**Project Github Page**

https://github.com/radical-cybertools/radical.ensemblemd

**Mailing Lists**

* For users: https://groups.google.com/d/forum/ensemblemd-users
* For developers: https://groups.google.com/d/forum/ensemblemd-devel

**Developer Wiki**

We maintain a wiki, mostly for internal planning and technical documents
at https://github.com/radical-cybertools/radical.ensemblemd/wiki

**Current Build Status**

* Release Branch 
    .. image:: https://travis-ci.org/radical-cybertools/radical.ensemblemd.svg?branch=master
        :target: https://travis-ci.org/radical-cybertools/radical.ensemblemd

* Development Branch
    .. image:: https://travis-ci.org/radical-cybertools/radical.ensemblemd.svg?branch=devel
        :target: https://travis-ci.org/radical-cybertools/radical.ensemblemd


Installation
************

To install EnsembleMD Toolkit in a virtual environment, open a terminal and run:

.. code-block:: bash

    virtualenv $HOME/enmd
    source $HOME/enmd/bin/activate
    git clone git@github.com:radical-cybertools/radical.ensemblemd.git
    cd radical.ensemblemd
    python setup.py install


Usage Examples
**************

.. toctree::
   :maxdepth: 2

   examples.rst

Use-Cases and Proof-of-Concepts
*******************************

.. toctree::
   :maxdepth: 2

   usecases.rst


Application Kernels and Plug-Ins
********************************

.. toctree::
   :maxdepth: 2

   kernels.rst

API Documentation
*****************

.. toctree::
   :maxdepth: 2

   library.rst

Developer Documentation
***********************

.. toctree::
   :maxdepth: 1

   developers.rst
