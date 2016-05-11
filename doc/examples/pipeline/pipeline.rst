.. _pipeline:

****************
Pipeline Example
****************

This example shows how to use the Ensemble Toolkit ``Pipeline`` pattern
to execute 16 concurrent pipeline of sequential tasks. In the first step of
each pipeline ``stage_1``, a 10 MB input file is generated and filled with
ASCII charaters. In the second step ``stage_2``, a character frequency analysis
if performed on this file. In the last step ``stage_3``, an SHA1 checksum is
calculated for the analysis result. The results of the frequency analysis and
the SHA1 checksums are copied back to the machine on which this script runs.

.. figure:: ../../images/pipeline_pattern.*
   :width: 300pt
   :align: center
   :alt: Pipeline Pattern

   Fig.: `The Pipeline Pattern.`

Run Locally
===========

.. warning:: In order to run this example, you need access to a MongoDB server and
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** View and download the example sources :ref:`below <example_source_pipeline>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python pipeline.py

Once the script has finished running, you should see the raw data of the
character analysis step (``cfreqs-XX.dat``) and the corresponding SHA1 checksums
(``cfreqs-XX.dat.sha1``) in the same directory you launched the script in.

Run Remotely
============

By default, the pipeline steps run on one core your local machine::

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

.. _example_source_pipeline:


Example Source
==============

:download:`Download example: pipeline.py <../../../examples/pipeline.py>`

.. literalinclude:: ../../../examples/pipeline.py
   :language: python
