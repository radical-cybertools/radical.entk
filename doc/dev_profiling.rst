Performance Profiling
=====================

During development of a new Ensemble MD Toolkit application, you will probably
come to the point where you want to develop a deeper understanding of how your
application behaves performance-wise.

Understanding an application's performance is the key to future design decisions
and performance optimizations.  Looking at the execution times of the
different stages and instances of your application is one of the key techniques
to achieve this.

Ensemble MD Toolkit provides two ways to access these performance numbers:
programmatically via the API or through the generation of a profile file.

Runtime Profile via API
-----------------------

Accessing the runtime profile of a pattern is very simple. After the a pattern
has been executed, i.e., the `run()` method returns, you can access the
`execution_profile` property:

.. code-block:: python

    p = Pattern(
      ...
    )
    cluster.run(p)

    print p.execution_profile

The `execution_profile` property returns a Python dictionary with performance
numbers. For the Simulation-Analysis pattern for example, this would look
something like this:

.. code-block:: python

    {'sim_2_end_time': 35.156960010528564, 'ana_2_end_time': 47.49368095397949, 'ana_1_end_time': 23.921995878219604, 'sim_1_end_time': 11.549329042434692}



Runtime Profile via File
------------------------

This way of profiling doesn't require any modification of your application
code and is entirely controlled via environment variables. This way you can
turn profiling off and on on-demand throughout the development process.

To get started, simply set the ``RADICAL_ENMD_PROFILING`` variable to ``1``
and run your application::

    RADICAL_ENMD_PROFILING=1 python examples/pipeline.py

Once your application has finished running, you will find a CSV file
`execution_profile_{time}.csv` (where time is the time of creation)
in your working directory containing some key execution timing.

Profile Inspection
------------------

The `ensemblemd-profile` tool installed with Ensemble MD Toolkit allows to
generate summary data from a profile data .csv file.

The tools takes either the name of a local .csv file or the URL of a remote .csv
file for analysis via the `-p` parameter:

.. code-block:: bash

   ensemblemd-profile -p execution_profile_2015-01-22T12:04:50.669529.csv

or

.. code-block:: bash

   ensemblemd-profile -p https://gist.githubusercontent.com/AntonsT/5385498524e2c9de1779/raw/c3191ac1117e462d85beff5c0d51e104bd179426/bag-of-tasts-execution-profile-stampede-128-1024

The output shows information about the runtimes of the individual stages /
instances of the pattern, e.g.,::

    1 Simulation step 1
    ------------------------------------------------------------------
     * Instances                : 16
     * Step start time          : 2015-01-22 11:01:53
     * Step stop time           : 2015-01-22 11:02:03
     * Step duration            : 0:00:10
     * Avg. duration / instance : 0:00:00.250000

    2 Analysis step 1
    ------------------------------------------------------------------
     * Instances                : 16
     * Step start time          : 2015-01-22 11:02:06
     * Step stop time           : 2015-01-22 11:02:22
     * Step duration            : 0:00:16
     * Avg. duration / instance : 0:00:01
