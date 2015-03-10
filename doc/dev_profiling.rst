Performance Profiling
=====================

During development of a new Ensemble MD Toolkit application, you will probably
come to the point where you want to develop a deeper understanding of how your
application behaves performance-wise.

Understanding an application's performance is the key to future design decisions
and performance optimizations.  Looking at the execution times of the
different stages and instances of your application is one of the key techniques
to achieve this.

Runtime Profile Access
----------------------

Accessing the runtime profile of a pattern is very simple. After the a pattern
has been executed, i.e., the `run()` method returns, you can access the
`execution_profile_dict` and the `execution_profile_dataframe` property:

.. code-block:: python

    p = Pattern(
      ...
    )
    cluster.run(p)

    print p.execution_profile_dict

The `execution_profile_dict` property returns a Python list with performance
numbers. For the Simulation-Analysis pattern for example, this would look
something like this:

.. code-block:: python

  [
  {'name': 'simulation_step_1',
   'timings': {
     'start_time': {
       'abs': datetime.datetime(2015, 3, 4, 17, 28, 29, 952194),
       'rel': datetime.timedelta(0, 11, 302058)
     },
     'end_time': {
       'abs': datetime.datetime(2015, 3, 4, 17, 28, 41, 159491),
       'rel': datetime.timedelta(0, 22, 509355)
     },
     'kernel_stagein_first_started': {
       'abs': None,
       'rel': None
     },
     'kernel_stagein_last_finished': {
       'abs': None,
       'rel': None
     },
     'kernel_exec_first_started': {
       'abs': datetime.datetime(2015, 3, 4, 18, 21, 39, 653000),
       'rel': datetime.timedelta(0)
     },
     'kernel_exec_last_finished': {
       'abs': datetime.datetime(2015, 3, 4, 18, 21, 49, 771000),
       'rel': datetime.timedelta(0, 10, 118000)
     },
     'kernel_stageout_last_finished': {
       'abs': None,
       'rel': None
     },
     'kernel_stageout_first_started': {
       'abs': None,
       'rel': None
     }
    }
  },
  {'name': 'analysis_step_1',
   'timings': {
     'start_time': {
       'abs': datetime.datetime(2015, 3, 4, 17, 28, 52, 332764),
       'rel': datetime.timedelta(0, 33, 682628)},
     'end_time': {
       'abs': datetime.datetime(2015, 3, 4, 17, 29, 3, 749004),
       'rel': datetime.timedelta(0, 45, 98868)
      }
      .
      .
      .
      .
    }
  }
  ]

Each entry in the array corresponds to one specific pattern entity, like for
example a simulation or an analysis step. The 'name' key references the entity.

The keys in the `timings` dictionary have the following meaning:

* ``start_time`` The time at which the entity started execution
* ``end_time`` The time at which the entity finished execution
* ``kernel_stagein_first_started`` The time the first kernel started transfering input data
* ``kernel_stagein_last_finished`` The time the last kernel finished transfering input data
* ``kernel_exec_first_started`` The time the first kernel started execution
* ``kernel_exec_last_finished`` The time the last kernel finished execution
* ``kernel_stageout_first_started`` The time the first kernel started transfering output data
* ``kernel_stageout_last_finished`` The time the last kernel finished transfering output data

The two sub-keys ``abs`` and ``rel`` point to the absolute time value and the
relative time value (relative to pattern execution start time ``t=0``) respectively.

If a kernel doesn't define input or output data transfer, the respective data
points will be ``None``.

.. figure:: images/profile.*
   :width: 360pt
   :align: center
   :alt: Profile.

   `Figure 1: Profile data.`

   The `execution_profile_dataframe` property returns a PANDAS DataFrame
   containing the same performance numbers as above.
   For the Simulation-Analysis pattern for example, this would look something like this::

            pattern_entity      value_type           lower           upper
     0   simulation_step_1    pattern_step 00:00:00.000191 00:00:11.301823
     1   simulation_step_1   units_stagein             NaT             NaT
     2   simulation_step_1      units_exec        00:00:00 00:00:10.104000
     3   simulation_step_1  units_stageout             NaT             NaT
     4     analysis_step_1    pattern_step 00:00:11.301903 00:00:22.473606
     5     analysis_step_1   units_stagein             NaT             NaT
     6     analysis_step_1      units_exec        00:00:00 00:00:10.109000
     7     analysis_step_1  units_stageout             NaT             NaT
     8   simulation_step_2    pattern_step 00:00:22.473685 00:00:34.801743
     9   simulation_step_2   units_stagein             NaT             NaT
     10  simulation_step_2      units_exec        00:00:00 00:00:10.120000
     11  simulation_step_2  units_stageout             NaT             NaT
     12    analysis_step_2    pattern_step 00:00:34.801825 00:00:46.202820
     13    analysis_step_2   units_stagein             NaT             NaT
     14    analysis_step_2      units_exec        00:00:00 00:00:10.115000
     15    analysis_step_2  units_stageout             NaT             NaT

Profile Inspection
------------------

.. warning::
   This will be replaced with new iPython-based method, once 6.2.1 is fixed.

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
