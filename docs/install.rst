.. _installation:

************
Installation
************

Installing Ensemble Toolkit
===========================

To install the Ensemble Toolkit in a virtual environment, open a terminal and run:

.. code-block:: bash

        virtualenv $HOME/myenv
        source $HOME/myenv/bin/activate
        cd $HOME
        git clone https://github.com/radical-cybertools/radical.ensemblemd.git
        cd radical.ensemblemd
        git checkout new_design
        pip install .

You can check the version of Ensemble MD Toolkit with the `entk-version` command-line tool. It should return 0.6.

.. code-block:: bash

        entk-version
        0.6


Installing rabbitmq
===================

Ensemble toolkit relies on RabbitMQ for message transfers. Installation 
instructions can be found at ```https://www.rabbitmq.com/download.html```. At 
the end of the installation run ```rabbitmq-server``` to start the server.

