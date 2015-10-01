.. _allpairs_example:

****************
AllPairs Example
****************

This example shows how to use the EnsembleMD Toolkit ``AllPairs`` pattern to
execute n(n-1)/2 simulation permutations of a set of n elements. In
the ``element_initialization`` step, the necessary files for the "all pair" comparison are
created. This step uses the ``misc.mkfile`` kernel to create random ASCII files.
Those files represent the elements of the set upon which the ``AllPairs`` pattern
will be executed. After the creation an ``element_comparison`` will be executed
for each unique pair of files (elements of the set). The ``element_comparison``
here executes the ``misc.diff`` kernel which returns the number of different lines
between the files. In the end the result files are downloaded to the folder from
which the script was run.

Run Locally
===========

.. warning:: In order to run this example, you need access to a MongoDB server and
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** View and download the example sources :ref:`below <example_source_all_pairs_pattern>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python allpairs_example.py

Once the script has finished running, you should see ``comparison-x-y.log`` files
in the same directory you launched the script in. This log files contain the number
of different lines between the compared files.

Run Remotely
============

By default, simulation and analysis steps run on one core your local machine::

    SingleClusterEnvironment(
        resource="localhost",
        cores=1,
        walltime=30,
        username=None,
        project=None
    )

You can change the script to use a remote HPC cluster and increase the number
of cores to see how this affects the runtime of the script as the individual
all pair instances can run in parallel::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        project=None # add your allocation or project id here if required
    )


.. _example_source_all_pairs_pattern:

Example Source
==============

:download:`Download allpairs_example.py <../../../examples/allpairs_example.py>`

.. literalinclude:: ../../../examples/allpairs_example.py
    :language: python
    :linenos:

