.. _usecase_extasy_gromacs_lsdmap:

*********************************
Use-Case: ExTASY (Gromacs-LSDMap)
*********************************

This example shows how to use the Ensemble Toolkit ``SimulationAnalysis``
pattern for the Gromacs-LSDMap usecase which has multiple Gromacs based Simulation
instances and a single LSDMap Analysis stage. Although the user is free to use
any method to mention the inputs, this usecase example uses two configuration
files - a RPconfig file (stampede.rcfg in this example) which consists of values
required to set up a pilot on the target machine, a Kconfig file (gromacslsdmap.wcfg
in this example) which consists of filenames/ parameter values required by
Gromacs/LSDMap. The description of each of these parameters is provided in their
respective config files.

In this particular usecase example, there are 16 simulation instances followed
by 1 analysis instance forming one iteration. The experiment is run for two
such iterations. The output of the second iteration is stored on the local
machine under a folder called "backup".


.. code-block:: none

	[S]    [S]    [S]    [S]    [S]    [S]    [S]
	 |      |      |      |      |      |      |
	 \-----------------------------------------/
						  |
						 [A]
						  |
	 /-----------------------------------------\
	 |      |      |      |      |      |      |
	[S]    [S]    [S]    [S]    [S]    [S]    [S]
	 |      |      |      |      |      |      |
	 \-----------------------------------------/
						  |
						 [A]
						  :

Run Locally
===========

.. warning:: In order to run this example, you need access to a MongoDB server and
			 set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
			 The format is ``mongodb://hostname:port``. Read more about it
			 MongoDB in chapter :ref:`envpreparation`.

.. warning:: Running locally would require you that have Gromacs and LSDMap installed on
			 your machine. Please go through Gromacs, LSDMap documentation to see how this
			 can be done.


By default, this example is setup to run on Stampede. You can also run it on your local
machine by setting the following parameters in your RPconfig file::

	REMOTE_HOST = 'localhost'
	UNAME       = ''
	ALLOCATION  = ''
	QUEUE       = ''
	WALLTIME    = 60
	PILOTSIZE   = 16
	WORKDIR     = None




Once the script has finished running, you should see a folder called "iter2" inside backup/
which would contain

Run Remotely
============

The script is configured to run on Stampede. You can increase the number
of cores to see how this affects the runtime of the script as the individual
simulations instances can run in parallel. You can try more variations
by modifying num_iterations(Kconfig), num_CUs (Kconfig), nsave (Kconfig), etc. ::

	cluster = ResourceHandle(
		resource="xsede.stampede",
		cores=16,
		walltime=30,
		username=None,  # add your username here
		project=None # add your allocation or project id here if required
		database_url=None # add your mongodb url
	)

**Step 1:** View and download the example sources :ref:`below <01_static_gromacs_lsdmap_loop>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

	RADICAL_ENMD_VERBOSE=info python 01_static_gromacs_lsdmap_loop.py --RPconfig stampede.rcfg --Kconfig gromacslsdmap.wcfg


Once the default script has finished running, you should see a folder called "iter2" inside backup/
which would contain the coordinate file for the next iteration(out.gro), output log of lsdmap (lsdmap.log)
and the weight file (weight.w).

.. _01_static_gromacs_lsdmap_loop:

Example Source
==============

:download:`Download example: 01_static_gromacs_lsdmap_loop.py <../../../usecases/extasy_gromacs_lsdmap/01_static_gromacs_lsdmap_loop.py>`

.. literalinclude:: ../../../usecases/extasy_gromacs_lsdmap/01_static_gromacs_lsdmap_loop.py
   :language: python
