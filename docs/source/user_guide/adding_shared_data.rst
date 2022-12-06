.. _uguide_add_shared_data:


******************
Adding Shared Data
******************

Data management is one of the important elements of any application. In many applications, there exist an initial set of
files, common to multiple tasks, that need to be transfered, once, to the remote machine.  In this section, we will take
a look at how we can manage data shared between multiple tasks to the remote machine by a one-time transfer.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <app_model>` and to have read through
            the :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.


You can download the complete code discussed in this section :download:`here <../../../examples/user_guide/add_shared_data.py>`
or find it in your virtualenv under ``share/radical.entk/user_guide/scripts``.

In the following example, we will create a Pipeline with one Stage and 10 tasks. The tasks concatenate two input files
and write the standard output to a file. Since the two input files are common between all the tasks, it will be efficient
to transfer those files only once to the remote machine and have the tasks copy the input files when being
executed.

Users can specify such shared data using the ``shared_data`` attribute of the AppManager object.

.. literalinclude:: ../../../examples/user_guide/add_shared_data.py
    :language: python
    :lines: 69-71
    :dedent: 4


The shared data can then be referenced for copying or linking using the ``$SHARED`` keyword for specifying the
file movement description of the task.

.. literalinclude:: ../../../examples/user_guide/add_shared_data.py
    :language: python
    :lines: 30-35
    :dedent: 4

In the example provided, the two files contain the words 'Hello' and 'World', respectively, and the output files
are expected to contain 'Hello World'

.. code-block:: bash

    python add_shared_data.py


Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_ENTK_VERBOSE=DEBUG``.


A look at the complete code in this section:

.. literalinclude:: ../../../examples/user_guide/add_shared_data.py
