.. _bag_of_tasks_example:

********************
Bag of Tasks Example
********************

This example shows how to use the Ensemble Toolkit :class:`.BagofTasks` pattern
to execute a single "Bag of Tasks". A Bag of Tasks is modeled as a
:class:`.BagofTasks` with just one stage. The "instances" of the  :class:`.BagofTasks`
corresponds to the number of tasks in the bag.

.. figure:: ../../images/bag_of_tasks.*
   :width: 300pt
   :align: center
   :alt: Bag of Tasks

   Fig.: `A Bag of Tasks pattern with 1 stage.`

Run Locally
===========

**Step 1:** View and download the example sources :ref:`below <example_source_bag_of_tasks>`.

**Step 2:** Run this example with ``RADICAL_ENTK_VERBOSE`` set to ``REPORT``.::

    RADICAL_ENTK_VERBOSE=REPORT python bag_of_tasks.py

Once the script has finished running, you should see the SHA1 checksums
genereated by the individual tasks  (``checksumXX.sha1``) in the in the same
directory you launched the script in.

You can generate a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.

Run on a Remote Cluster
=======================

By default, this Bag of Tasks runs on one core on your local machine::

    SingleClusterEnvironment(
        resource="localhost",
        cores=1,
        walltime=30,
        username=None,
        allocation=None
    )

You can change the script to use a remote HPC cluster and increase the number
of cores to see how this affects the runtime of the script as the individual
tasks in the bag can run in parallel::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        allocation=None # add your allocation or project id here if required
    )

.. _example_source_bag_of_tasks:


Example Source
==============

:download:`Download example: bag_of_tasks.py <../../../examples/bag_of_tasks.py>`

.. literalinclude:: ../../../examples/bag_of_tasks.py
   :language: python
