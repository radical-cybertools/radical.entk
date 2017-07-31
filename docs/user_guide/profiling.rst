.. _uguide_profiling:


*********
Profiling
*********

In this section, we will take a look at how we can profiling our executions using the ``Profiler`` in Ensemble Toolkit.
We will modify our script from the :ref:`Adding Stages <uguide_add_stages>` section.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <overview>` and to have read through the :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.

You can download the complete code discussed in this section :download:`here <../../examples/user_guide/profiling.py>` or find it in 
your virtualenv under ``share/radical.entk/user_guide/scripts``.

        
Below, you can see the code snippet that shows how you can profile specific tasks. 

First we append the uid of all the tasks in the first stage to a list.

.. literalinclude:: ../../examples/user_guide/profiling.py
    :language: python
    :lines: 16-33
    :dedent: 4

Next, we append the uid of all the tasks in the second stage to another list.

.. literalinclude:: ../../examples/user_guide/profiling.py
    :language: python
    :lines: 39-56
    :dedent: 4

After the execution of the workflow is complete, we use the ``Profiler`` module in Ensemble Toolkit, specifically its
``duration`` method to find the execution duration of all the tasks in the first stage and then the same for the 
tasks in the second stage.

.. literalinclude:: ../../examples/user_guide/profiling.py
    :language: python
    :lines: 89-95
    :dedent: 4

We get the execution duration since we mentioned the states as "SCHEDULING" and "EXECUTED" which is the execution
time from the perspective of Ensemble Toolkit. You can use the same profiler to obtain durations between two events or
a state and an event. You can find a list of all the states in the UML se


Before running the script, you will have to enable the profiling variable. To do this, run the following command
in the same terminal where you have the virtualenv sourced:

::

    export RADICAL_ENTK_PROFILE=True



To run the script, simply execute the following from the command line:

.. tip:: For the purposes of this user guide, we have a MongoDB setup to use. Please run the following command to use 
        it::

            export RADICAL_PILOT_DBURL="mongodb://138.201.86.166:27017/ee_exp_4c"

.. code-block:: bash

    python profiling.py


Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_ENTK_VERBOSE=DEBUG``.

A look at the complete code in this section:

.. literalinclude:: ../../examples/user_guide/profiling.py