.. _usecase_extasy_coco_amber:


*****************************
Use-Case: ExTASY (Amber-CoCo)
*****************************

This example shows how to use the Ensemble Toolkit ``SimulationAnalysis``
pattern for the Amber-CoCo usecase which has multiple Amber based Simulation
instances and a single CoCo Analysis stage. Although the user is free to use
any method to mention the inputs, this usecase example uses two configuration
files - a RPconfig file (stampede.rcfg in this example) which consists of
values required to set up a pilot on the target machine, a Kconfig file
(cocoamber.wcfg in this example) which consists of filenames/ parameter values
required by Amber/CoCo. The description of each of these parameters is provided
in their respective config files.

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

.. warning:: Running locally would require you that have Amber and CoCo installed on
			 your machine. Please go through Amber, CoCo documentation to see how this
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
which would contain 16 .ncdf files which are the output of the second simulation stage. You
see 16 .ncdf files since there were 16 simulation instances.

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

You can download the tarball containing the scripts and the input files using,

.. code-block:: bash

	wget https://github.com/radical-cybertools/radical.ensemblemd/blob/master/usecases/extasy_coco_amber.tar.gz?raw=true


You can run the script using the following command:

.. code-block:: bash

	RADICAL_ENMD_VERBOSE=REPORT python 01_static_amber_coco_loop.py --RPconfig stampede.rcfg --Kconfig cocoamber.wcfg


Once the default script has finished running, you should see a folder called "iter2" inside backup/
which would contain 16 .ncdf files which are the output of the second simulation stage. You
see 16 .ncdf files since there were 16 simulation instances.

.. _01_static_amber_coco_loop:

Example Source
==============

:download:`Download example: 01_static_amber_coco_loop.py <../../../usecases/extasy_coco_amber/01_static_amber_coco_loop.py>`

.. literalinclude:: ../../../usecases/extasy_coco_amber/01_static_amber_coco_loop.py
   :language: python
