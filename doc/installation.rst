.. _envpreparation:

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