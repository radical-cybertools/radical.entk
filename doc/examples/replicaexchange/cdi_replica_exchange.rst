.. _usecase_cdi_replica_exchange:

*****************************************************************************************
Use-Case: Synchronous Temperature Exchange with NAMD using Ensemble Exchange pattern
*****************************************************************************************

This script is an example of simple application using the Ensemble Toolkit ``ReplicaExchange``
pattern for synchronous termerature-exchange RE simulation. All files referenced in the following 
description are available for download at the end of the page. Demonstrated RE simulation involves 32 
replicas and performs a total of 5 synchronous simulation cycles. Here exchange step is performed 
on target resource, which corresponds to ``static_pattern_2`` execution plugin. As MD application 
kernel in this use-case is used NAMD. Input files can be found  in ``namd_inp`` 
directory. Shared input files used by each replica are specified in prepare_shared_data() method. 
This method is called when specified files are to be transferred to the target resource. For this 
use-case these files are ``alanin.psf`` structure file, ``unfolded.pdb`` coordinates file and 
``alanin.params`` parameters file. After shared input files are transferred, for each replica is 
created input file using a template ``namd_inp/alanin_base.namd``. In this 
file are specified replica simulation parameters according to their initial values. After input 
files are created ``prepare_replica_for_md()`` is called and MD step is performed on a target 
resource. Then follows Exchange step. For each replica preparation required to perform this step 
on a target resource is specified in ``prepare_replica_for_exchange()``. After Exchange step is 
finished, final exchange procedure is performed locally according to Gibbs sampling method. This 
procedure is defined in ``get_swap_matrix()``, ``exchange()``, ``weighted_choice_sub()`` and 
``perform_swap()`` methods. After exchange procedure is finished the next MD run is performed and 
the process is then repeated. For remote exchange step is used ``namd_matrix_calculator.py`` 
python script. This script calculates one swap matrix column for replica by retrieving temperature 
and potential energy from simulation output file .history file.

Run Locally
===========

.. warning:: In order to run this example, you need access to a MongoDB server and
			 set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
			 The format is ``mongodb://hostname:port``. Read more about it
			 MongoDB in chapter :ref:`envpreparation`.
			 In addition you need to have a local NAMD installation and NAMD should be
			 invocable by calling ``namd2`` from terminal.

**Step 1:** View and download the example sources :ref:`below <example_repex_usecase>`.


**Step 2:** Change number of replicas to 4 at line 94 of ``replica_exchange_mode_1.py``.
In principle it is suggested to set a ratio of compute cores to replicas as 1:2.


**Step 3:** Run this example with ``RADICAL_ENTK_VERBOSE`` set to ``REPORT``::

	RADICAL_ENTK_VERBOSE=REPORT python replica_exchange_mode_1.py


**Step 4:** Verify presence of generated input files alanin_base_x_y.namd and alanin_base_x_y.history
output files where x is replica id and y is cycle number.

You can generate a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.

Run Remotely
============

By default, this use-case runs on your local machine

.. literalinclude:: ../../../usecases/cdi_replica_exchange/replica_exchange_mode_1.py
	:language: python
	:lines: 418-425
	:dedent: 2

You can change the script to use a remote HPC cluster.

.. code-block:: python

	cluster = ResourceHandle(
		resource="xsede.stampede",
		cores=16,
		walltime=30,
		username=None,  # add your username here
		project=None # add your allocation or project id here if required
		database_url=None # add your mongodb url
	)

Number of replicas and number of simulation cycles are defined in constructor of RePattern class.
Please change the number of cores and the number of replicas according to suggested ratio of 1:2.

.. _example_repex_usecase:

Example Source
==============

:download:`Download example: replica_exchange_mode_1.py <../../../usecases/cdi_replica_exchange/replica_exchange_mode_1.py>`

:download:`Download matrix calculator: namd_matrix_calculator.py <../../../usecases/cdi_replica_exchange/namd_matrix_calculator.py>`

:download:`Download NAMD input files: namd_inp.tar.gz <../../../usecases/cdi_replica_exchange/namd_inp.tar.gz>`

.. warning:: Before running this example you must uncompress the ``namd_inp.tar.gz`` 
			 archive in your working directory.

.. literalinclude:: ../../../usecases/cdi_replica_exchange/replica_exchange_mode_1.py
   :language: python
