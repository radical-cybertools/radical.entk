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

.. note:: You can use the Python pprint library to make the output a bit more pleasing to the eye.

.. code-block:: python

    import pprint

    ...

    p = Pattern(
      ...
    )
    cluster.run(p)

    pp = pprint.PrettyPrinter()
    pp.pprint(p.execution_profile_dict)

The `execution_profile_dict` property returns a Python list with performance
numbers. For the Simulation-Analysis pattern for example, this would look
something like this:

.. code-block:: python

  [{'name': 'simulation_iteration_1',
    'timings': {'end_time': {'abs': datetime.datetime(2015, 3, 10, 23, 5, 46, 453547),
                             'rel': datetime.timedelta(0, 14, 58117)},
                'first_data_stagein_started': {'abs': datetime.datetime(2015, 3, 10, 22, 5, 32, 951000),
                                               'rel': datetime.timedelta(0)},
                'first_data_stageout_started': {'abs': None, 'rel': None},
                'first_execution_started': {'abs': datetime.datetime(2015, 3, 10, 22, 5, 36, 45000),
                                            'rel': datetime.timedelta(0, 3, 94000)},
                'last_data_stagein_finished': {'abs': datetime.datetime(2015, 3, 10, 22, 5, 34, 438000),
                                               'rel': datetime.timedelta(0, 1, 487000)},
                'last_data_stageout_finished': {'abs': None, 'rel': None},
                'last_execution_finished': {'abs': datetime.datetime(2015, 3, 10, 22, 5, 46, 70000),
                                            'rel': datetime.timedelta(0, 13, 119000)},
                'start_time': {'abs': datetime.datetime(2015, 3, 10, 23, 5, 32, 395504),
                               'rel': datetime.timedelta(0, 0, 74)}}},
   {'name': 'analysis_iteration_1',
    'timings': {'end_time': {'abs': datetime.datetime(2015, 3, 10, 23, 6, 0, 220123),
                             'rel': datetime.timedelta(0, 27, 824693)},
                'first_data_stagein_started': {'abs': datetime.datetime(2015, 3, 10, 22, 5, 47, 55000),
                                               'rel': datetime.timedelta(0)},
                'first_data_stageout_started': {'abs': None, 'rel': None},
                'first_execution_started': {'abs': datetime.datetime(2015, 3, 10, 22, 5, 49, 734000),
                                            'rel': datetime.timedelta(0, 2, 679000)},
                'last_data_stagein_finished': {'abs': datetime.datetime(2015, 3, 10, 22, 5, 48, 407000),
                                               'rel': datetime.timedelta(0, 1, 352000)},
                'last_data_stageout_finished': {'abs': None, 'rel': None},
                'last_execution_finished': {'abs': datetime.datetime(2015, 3, 10, 22, 5, 59, 842000),
                                            'rel': datetime.timedelta(0, 12, 787000)},
                'start_time': {'abs': datetime.datetime(2015, 3, 10, 23, 5, 46, 453705),
                               'rel': datetime.timedelta(0, 14, 58275)}}},
        ...
  ]


Each entry in the array corresponds to one specific pattern entity, like for
example a simulation or an analysis step. The 'name' key references the entity.

The keys in the `timings` dictionary have the following meaning:

* ``start_time`` The time at which the entity started execution
* ``end_time`` The time at which the entity finished execution
* ``first_data_stagein_started`` The time the first kernel started transfering input data
* ``last_data_stagin_finished`` The time the last kernel finished transfering input data
* ``first_execution_started`` The time the first kernel started execution
* ``last_execution_finished`` The time the last kernel finished execution
* ``first_data_stageout_started`` The time the first kernel started transfering output data
* ``last_data_stageout_finished`` The time the last kernel finished transfering output data

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
containing the same performance numbers as above:

.. code-block:: python

   p = Pattern(
     ...
   )
   cluster.run(p)

   print p.execution_profile_dataframe

For the Simulation-Analysis pattern for example, this would look something like this::

       pattern_entity          value_type     first_started_abs          last_finished_abs             first_started_rel  last_finished_rel
  0   simulation_iteration_1   pattern_step   2015-03-10 22:59:27.686589   2015-03-10 22:59:40.402068    00:00:00.000105    00:00:12.715584
  1   simulation_iteration_1   data_stagein   2015-03-10 21:59:28.241000   2015-03-10 21:59:29.623000           00:00:00    00:00:01.382000
  2   simulation_iteration_1      execution   2015-03-10 21:59:29.987000   2015-03-10 21:59:40.102000    00:00:01.746000    00:00:11.861000
  3   simulation_iteration_1  data_stageout                          NaT                          NaT                NaT                NaT
  4     analysis_iteration_1   pattern_step   2015-03-10 22:59:40.402215   2015-03-10 22:59:52.875189    00:00:12.715731    00:00:25.188705
  5     analysis_iteration_1   data_stagein   2015-03-10 21:59:40.858000   2015-03-10 21:59:41.959000           00:00:00    00:00:01.101000
  6     analysis_iteration_1      execution   2015-03-10 21:59:42.358000   2015-03-10 21:59:52.468000    00:00:01.500000    00:00:11.610000
  7     analysis_iteration_1  data_stageout                          NaT                          NaT                NaT                NaT
  8   simulation_iteration_2   pattern_step   2015-03-10 22:59:52.875332   2015-03-10 23:00:05.215202    00:00:25.188848    00:00:37.528718
  9   simulation_iteration_2   data_stagein   2015-03-10 21:59:53.434000   2015-03-10 21:59:54.545000           00:00:00    00:00:01.111000
  10  simulation_iteration_2      execution   2015-03-10 21:59:54.831000   2015-03-10 22:00:04.863000    00:00:01.397000    00:00:11.429000
  11  simulation_iteration_2  data_stageout                          NaT                          NaT                NaT                NaT
  12    analysis_iteration_2   pattern_step   2015-03-10 23:00:05.215301   2015-03-10 23:00:17.732945    00:00:37.528817    00:00:50.046461
  13    analysis_iteration_2   data_stagein   2015-03-10 22:00:05.733000   2015-03-10 22:00:06.557000           00:00:00    00:00:00.824000
  14    analysis_iteration_2      execution   2015-03-10 22:00:07.315000   2015-03-10 22:00:17.428000    00:00:01.582000    00:00:11.695000
  15    analysis_iteration_2  data_stageout                          NaT                          NaT                NaT                NaT


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
