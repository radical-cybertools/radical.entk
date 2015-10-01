.. _replica_exchange_remote (generic):

*********************************************************************
Synchronous Replica Exchange Example with 'remote' Exchange (generic)
*********************************************************************

This example shows how to use the Ensemble MD Toolkit ``ReplicaExchange`` pattern.
Demonstrated RE simulation involves 16 replicas and performs a total of 3 synchronous simulation
cycles. Here exchange step is performed on target resource, which corresponds to ``static_pattern_2`` execution
plugin. Firstly, for each replica is generated dummy ``md_input_x_y.md`` input file and ``shared_md_input.dat``
shared file. Each of ``md_input_x_y.md`` files contains 500 randomly generated numbers, but ``shared_md_input.dat``
contains 500 characters which are both numbers and letters. As MD kernel in this example is used
``misc.ccount`` kernel which counts the number of occurrences of all characters in a given file.
As input file for this kernel is supplied previously generated ``md_input_x_y.md`` file. ``misc.ccount``
kernel produces ``md_input_x_y.out`` file, which is transferred to current working directory.
For exchange step is used ``md.re_exchange`` kernel which is supplied with ``matrix_calculator.py``
python script. This script is executed on target resource and simulates collection of output
parameters produced by MD step, which are required for exchange step. In this example ``matrix_calculator.py``
returns dummy parameter, which is a randomly generated number. This number does not affect the exchange
probability nor does it affect the choice of replica to perform an exchange with. Replica to perform an
exchange with is chosen randomly. Dummy replica parameter named ``parameter`` is exchanged during the
exchange step. Exchanges of ``parameter`` do not affect next simulation cycle.

.. figure:: ../../images/replica_exchange_pattern.*
   :width: 300pt
   :align: center
   :alt: Replica Exchange Pattern

   Fig.: `Replica Exchange Pattern.`

Run Locally
===========

.. warning:: In order to run this example, you need access to a MongoDB server 
             and set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** Download matrix calculator from :ref:`here <example_matrix_calculator>`.

**Step 2:** View and download the example :ref:`below <example_replica_exchange_b>`.

**Step 3:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python replica_exchange_b.py

TODO Antons: describe how to check the output.

Run Remotely
============

By default, the exchange steps and the analysis run on one core your local machine::

    SingleClusterEnvironment(
        resource="localhost",
        cores=1,
        walltime=30,
        username=None,
        project=None
    )

You can change the script to use a remote HPC cluster and increase the number of cores to see how this affects the runtime of the script as the individual
pipeline instances can run in parallel::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        project=None # add your allocation or project id here if required
    )

.. _example_matrix_calculator:

:download:`Download: matrix_calculator.py <../../../examples/matrix_calculator.py>`

.. _example_replica_exchange_b:

:download: `Download: replica_exchange_b.py <../../../examples/replica_exchange_b.py>`


Example Source
^^^^^^^^^^^^^^

.. literalinclude:: ../../../examples/replica_exchange_b.py
    :language: python
    :linenos:

