RADICAL Ensemble MD Toolkit
###########################

.. figure:: images/oxytocin.*
   :width: 160pt
   :align: right

The radical.ensemblemd package is a Python API and library for developing and 
executing large-scale ensemble-based Molecular Dynamics simulations and workflows


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

To install the EnsembleMD Toolkit Python modules in a virtual environment, 
open a terminal and run:

.. code-block:: bash

    virtualenv $HOME/EnMDToolkit
    source $HOME/EnMDToolkit/bin/activate
    pip install --upgrade git+https://github.com/radical-cybertools/radical.ensemblemd.git@master#egg=radical.ensemblemd

You can check the version of the installed EnsembleMD Toolkit like this:

.. code-block:: bash

    python -c "import radical.ensemblemd; print radical.ensemblemd.version"

.. include:: examples_toc.rst

Use-Cases and Proof-of-Concepts
*******************************

.. toctree::
   :maxdepth: 2

   usecases.rst


Application Kernels
*******************

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
