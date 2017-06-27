.. _replica_exchange_remote:

*********************************************************************
Synchronous Replica Exchange Example with 'remote' Exchange
*********************************************************************

This example shows how to use the Ensemble Toolkit ``ReplicaExchange`` pattern with artificial workload.
Demonstrated RE simulation involves 8 replicas and performs a total of 3 synchronous simulation
cycles. Here exchange step is performed on target resource. Firstly, for each replica is generated dummy ``md_input_x_y.md`` input file and ``shared_md_input.dat``
shared file. Each of ``md_input_x_y.md`` files contains 500 randomly generated numbers, but ``shared_md_input.dat``
contains 500 characters which are both numbers and letters. As MD kernel in this example is used
``misc.ccount`` kernel which counts the number of occurrences of all characters in a given file.
As input file for this kernel is supplied previously generated ``md_input_x_y.md`` file. ``misc.ccount``
kernel produces ``md_input_x_y.out`` file, which is transferred back to localhost.
For exchange step is used ``md.re_exchange`` kernel which is supplied with ``matrix_calculator.py``
python script. This script is executed on target resource and simulates collection of output
parameters produced by MD step, which are required for exchange step. In this example ``matrix_calculator.py``
returns dummy parameter, which is a randomly generated number. This number does not affect the exchange
probability nor does it affect the choice of a replica to perform an exchange with. Pairs of replicas for exchanges are chosen randomly. Dummy replica parameter named ``parameter`` is exchanged during the
exchange step. Exchanges of ``parameter`` do not affect next simulation cycle.

.. figure:: ../../images/replica_exchange_pattern.*
   :width: 300pt
   :align: center
   :alt: Replica Exchange Pattern

   Fig.: `Replica Exchange Pattern.`

Run Locally
===========


**Step 1:** Download matrix calculator from :ref:`here <example_matrix_calculator>`  or find it in 
your virtualenv under ``share/radical.ensemblemd/examples/matrix_calculator.py``.

**Step 2:** View and download the example :ref:`below <example_replica_exchange_b>`  or find it in 
your virtualenv under ``share/radical.ensemblemd/examples/replica_exchange_b.py``.

**Step 3:** Run this example with ``RADICAL_ENTK_VERBOSE`` set to ``REPORT``::

	RADICAL_ENTK_VERBOSE=REPORT python replica_exchange_b.py

After execution is done, in working directory you should have 24 md_input_x_y.md files and 24 md_input_x_y.out files where x in {0,1,2} and y in {0,1,...7}. File with extension .md is replica input file and with extension .out is output file providing number of occurrences of each character.

You can generate a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.

Run Remotely
============

By default, the exchange steps and the analysis run on one core your local machine.

.. code-block:: python

	ResourceHandle(
		resource="local.localhost",
		cores=1,
		walltime=30,
		username=None,
		project=None
	)

You can change the script to use a remote HPC cluster and increase the number of cores to see how this affects the runtime of the script as the individual
pipeline instances can run in parallel.

.. code-block:: python

	ResourceHandle(
		resource="xsede.stampede",
		cores=16,
		walltime=30,
		username=None,  # add your username here
		project=None # add your allocation or project id here if required
	)



Example Source
==============

.. _example_matrix_calculator:

:download:`Download: matrix_calculator.py <../../../examples/matrix_calculator.py>`

.. _example_replica_exchange_b:

:download:`Download: replica_exchange_b.py <../../../examples/replica_exchange_b.py>`


.. literalinclude:: ../../../examples/replica_exchange_b.py
	:language: python

