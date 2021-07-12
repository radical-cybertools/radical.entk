.. _uguide_add_data:


***********
Adding Data
***********

Data management is one of the important elements of any application. In this section, we will take a look at how we can 
manage data between tasks.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <app_model>` and to have read through 
            the :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.

You can download the complete code discussed in this section :download:`here <../../examples/user_guide/add_data.py>` or
find it in your virtualenv under ``share/radical.entk/user_guide/scripts``.

In the following example, we will create a Pipeline of two Stages, each with one Task. In the first stage, we will
create a file of size 1MB. In the next stage, we will perform a character count on the same file and write to another
file. Finally, we will bring that output to the current location. 

Since we already know how to create this workflow, we simply present the code snippet concerned with the data movement. 
The task in the second stage needs to perform two data movements: a) copy the data created in the first stage to its 
current directory and b) bring the output back to the directory where the script is executed. Below we present the 
statements that perform these operations.

.. literalinclude:: ../../examples/user_guide/add_data.py
    :language: python
    :lines: 46-53
    :dedent: 4

Intermediate data are accessible through a unique identification formed by
`$Pipeline_%s_Stage_%s_Task_%s/{filename}` where `%s` is replaced by entity
name of pipeline, stage and task. If name is not given, `.uid` is available to
locate files across tasks. :ref:`task_api` has more information about the use
of API.


Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_LOG_LVL=DEBUG``.

.. literalinclude:: ../../examples/user_guide/add_data.py

Handling data from login node
=============================

The following example shows how to configure a task to fetch data at runtime, when compute nodes do not have internet access.

.. code-block:: python

    t = Task()
    t.name = 'download-task'
    t.executable = '/usr/bin/ssh'
    t.arguments = ["<username>@<hostname>",
                   "'bash -l /path/to/download_script.sh <input1> <input2>'"]
    t.download_output_data = ['STDOUT', 'STDERR']
    t.cpu_reqs = {'cpu_processes': 1,
                  'cpu_process_type': None,
                  'cpu_threads': 1,
                  'cpu_thread_type': None}


.. note:: ``bash -l`` makes the shell act as if it had been directly invoked
          by logging in.

.. note:: Verify that the command ``ssh localhost hostname`` works on the 
          login node without a password prompt. If you are asked for a 
          password, please create an ssh keypair with the command 
          ``ssh-keygen``. That should create two keys in ``~/.ssh``, named 
          ``id_rsa`` and ``id_rsa.pub``. Now, execute the following commands:

          .. code-block:: bash

              cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys
              chmod 0600 $HOME/.ssh/authorized_keys
