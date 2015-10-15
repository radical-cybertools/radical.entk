.. _usecase_haussdorff:

*******************************************
Use-Case: "Haussdorff Distance Calculation"
*******************************************

This Use Case uses the AllPairs pattern provided be Ensemble MD Toolkit to calculate
the Hausdorff Distance between the trajectories of all atom combinations. Each
trajectory file corresponds to the trajectory of an atom. 

Specifically the script defines the QPC kernel (lines 25-88), based on the 
instructions provided in 7.4.1. Writing New Application Kernels, which stages 
the trajectory input files in order for the Hausdorff Distance to be calculated.

The kernel (MyHausdorff) which defines the application workload for the Hausdorff
distances is created in lines 91-170.

Input files can be found `here <http://eceweb1.rutgers.edu/~ip176/>`_. The 
``traj_flat<>.npz.npy`` files are the trajectory files that are used as input for
the Hausdorrf Distance Calculation. ``hausdorff_kernel.py`` contains the calculation
method programmed in Python for the Hausdorff Distance Calculation.

The ``element_initialization`` step of the pattern uses the My_QPC kernel to move
the input necessary files for the "all pair" Hausdorff Distance Calculation.
This step uses the ``my.qpc`` kernel that is defined in this script. After the 
initialization an ``element_comparison`` will be executed for each unique pair 
of files (elements of the set). The ``element_comparison`` here executes the 
``my.hausdorff`` kernel which returns the distance between two trajetories. In 
the end the result files are downloaded to the folder from which the script was run.


Run Locally
===========

.. warning:: In order to run this example, you need access to a MongoDB server and
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** Download the example source code :ref:`below <source_hausdorff>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python hausdorff_example.py

Once the script has finished running, you should see ``distance-x-y.log`` files
in the same directory you launched the script in. This log files contain the 
calculated distance between the atoms the trajectories where in files 
traj_flatx.npz.npy and traj_flaty.npz.npy

Run Remotely
============

By default, Hausdorff Distance script runs on one core your local machine::

    SingleClusterEnvironment(
        resource="localhost",
        cores=1,
        walltime=30,
        username=None,
        allocation=None
    )

You can change the script to use a remote HPC cluster and increase the number
of cores to see how this affects the runtime of the script as the individual
all pair instances can run in parallel::

    SingleClusterEnvironment(
        resource="xsede.stampede",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        allocation=None # add your allocation or project id here if required
    )

.. _source_hausdorff:



Example Source
==============


:download:`Download hausdorff_example.py <../../../usecases/hausdorff/hausdorff_example.py>`

:download:`Download hausdorff_kernel.py <../../../usecases/hausdorff/hausdorff_kernel.py>`


.. literalinclude:: ../../../usecases/hausdorff/hausdorff_example.py
    :language: python
    :linenos:
