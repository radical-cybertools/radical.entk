.. _replica_exchange_local:

********************************************************************
Synchronous Replica Exchange Example with 'local' Exchange
********************************************************************
 
This example shows how to use the Ensemble Toolkit ``ReplicaExchange`` pattern.
Demonstrated RE simulation involves 16 replicas and performs a total of 3 synchronous simulation cycles.
Here exchange step is performed locally, which corresponds to ``static_pattern_1`` execution plugin.
Firstly, for each replica is generated dummy ``md_input_x_y.md``
input file. Each of these files contains 500 randomly generated numbers. As MD kernel in this example
is used ``misc.ccount`` kernel which counts the number of occurrences of all characters in a given file.
As input file for this kernel is supplied previously generated ``md_input_x_y.md`` file. ``misc.ccount``
kernel produces ``md_input_x_y.out`` file, which is transferred to current working directory.
Dummy replica parameter named ``parameter`` is exchanged during the exchange step. Exchanges
of ``parameter`` do not affect next simulation cycle. Replica to perform an exchange with is
chosen randomly.

.. figure:: ../../images/replica_exchange_pattern.*
   :width: 300pt
   :align: center
   :alt: Replica Exchange Pattern

   Fig.: `Replica Exchange Pattern.`

Run Locally
===========

.. warning:: In order to run this example, you need access to a MongoDB server and
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** View and download the example sources :ref:`below <example_replica_exchange>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python replica_exchange.py

Run Remotely
============

By default, the exchange steps run on one core your local machine::

    SingleClusterEnvironment(
        resource="localhost",
        cores=1,
        walltime=30,
        username=None,
        project=None
    )

You can change the script to use a remote HPC cluster and increase the number
of cores to see how this affects the runtime of the script as the individual
pipeline instances can run in parallel::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        project=None # add your allocation or project id here if required
    )

.. _example_replica_exchange:


Example Source
==============

:download: `Download: replica_exchange.py <../../../examples/replica_exchange.py>`

.. literalinclude:: ../../../examples/replica_exchange.py
    :language: python

