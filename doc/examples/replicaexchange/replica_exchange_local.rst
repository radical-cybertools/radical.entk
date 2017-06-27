.. _replica_exchange_local:

*****************************************
Synchronous Ensemble Exchange Example 
*****************************************
 
This example shows how to use the Ensemble Toolkit ``ReplicaExchange`` pattern with artificial workload. Demonstrated RE simulation involves 8 ensemble members and performs a total of 3 synchronous simulation cycles. In this example exchange step is performed locally. Firstly, for each ensemble member is generated dummy ``md_input_x_y.md`` input file. Each of these files contains 500 randomly generated numbers. As MD kernel in this example is used ``misc.ccount`` kernel which counts the number of occurrences of all characters in a given file. As input file for this kernel is supplied previously generated ``md_input_x_y.md`` file. ``misc.ccount`` kernel produces ``md_input_x_y.out`` file, which is transferred back to localhost. Dummy ensemble member parameter named ``parameter`` is exchanged during the exchange step. Exchanges of ``parameter`` have no effect on the next simulation cycle. A pair of ensemble members for exchange is chosen randomly.

.. figure:: ../../images/replica_exchange_pattern.*
   :width: 300pt
   :align: center
   :alt: Replica Exchange Pattern

   Fig.: `Ensemble Exchange Pattern.`

Run Locally
===========


.. warning:: In order to run this example, you need access to a MongoDB server and
			 set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
			 The format is ``mongodb://hostname:port``. Read more about it
			 MongoDB in chapter :ref:`envpreparation`.


**Step 1:** View and download the example sources :ref:`below <example_ensemble_exchange>`  or find it in 
your virtualenv under ``share/radical.ensemblemd/examples/ensemble_exchange.py``.


**Step 2:** Run this example with ``RADICAL_ENTK_VERBOSE`` set to ``REPORT``::

	RADICAL_ENTK_VERBOSE=REPORT python ensemble_exchange.py

After execution is done, in working directory you should have 24 md_input_x_y.md files and 24 md_input_x_y.out files where x in {0,1,2} and y in {0,1,...7}. File with extension .md is input file for the ensemble member and with extension .out is output file providing number of occurrences of each character.

You can generate a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.

Run Remotely
============

By default, the exchange steps run on one core your local machine

.. literalinclude:: ../../../examples/ensemble_exchange.py
	:lines: 213-223
	:language: python
	:dedent: 2


You can change the script to use a remote HPC cluster and increase the number
of cores to see how this affects the runtime of the script as the individual
pipeline instances can run in parallel::

	cluster = ResourceHandle(
		resource="xsede.stampede",
		cores=16,
		walltime=30,
		username=None,  # add your username here
		project=None # add your allocation or project id here if required
		database_url=None # add your mongodb url
	)

.. _example_ensemble_exchange:

Example Source
==============

:download:`Download: ensemble_exchange.py <../../../examples/ensemble_exchange.py>`

.. literalinclude:: ../../../examples/ensemble_exchange.py
	:language: python

