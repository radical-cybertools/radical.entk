.. _uguide_add_data:


***********
Adding Data
***********

Data movement is one of the important elements of any application. In this section, we will take a look at how we can 
move data between different tasks.

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
    :lines: 46-49
    :dedent: 4


.. code-block:: bash

    python add_data.py


Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_ENTK_VERBOSE=DEBUG``.


A look at the complete code in this section:

.. literalinclude:: ../../examples/user_guide/add_data.py

Handling data from login node
=============================

The following example shows how to configure a task to fetch data during runtime when compute nodes do not have internet access.

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
          by login.

.. note:: Need to make sure that, on the login node, the following works
          without a password prompt: ``ssh localhost hostname``. If a password
          is prompt then need to create a ssh keypair (ssh-keygen), which should
          by default create two keys in ``~/.ssh``, likely named ``id_rsa`` and
          ``id_rsa.pub`` (the key names may differ, depending on system
          defaults IIRC), and the following commands should be run after:
          ``cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys``
          ``chmod 0600 $HOME/.ssh/authorized_keys``
