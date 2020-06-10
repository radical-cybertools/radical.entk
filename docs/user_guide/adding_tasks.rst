.. _uguide_add_tasks:


************
Adding Tasks
************

In this section, we will take a look at how we can add more tasks to our base script from the 
:ref:`Getting Started <uguide_get_started>` section.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <app_model>` and to have read through 
    the :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.

You can download the complete code discussed in this section :download:`here <../../examples/user_guide/add_tasks.py>` 
or find it in your virtualenv under ``share/radical.entk/user_guide/scripts``.        

Below, you can see the code snippet that shows how you can create more Task objects
and **add** them to the Stage using the **add_task()** method.

.. literalinclude:: ../../examples/user_guide/add_tasks.py
    :language: python
    :lines: 24-35
    :dedent: 4


.. code-block:: bash

    python add_tasks.py

.. note:: Individual Task must be created each time in order to treat all the items uniquely inside a Stage. For example,
          for loop can be used for creating multiple tasks and creating Task object needs to be included inside iteration.

.. code-block:: python

    # Five tasks to be added
    s = Stage()
    task_dict = {
        't.cpu_reqs': {
        'processes'          : 10,
        'threads_per_process': 1,
        'process_type'       : "MPI",
        'thread_type'        : "OpenMP"
        }}

    for i in range(5): 
        task_dict['name']      = "task-{}".format(i)
        task_dict['arguments'] = ["file-{}".format(i)]
        # Creating new Task object and adding to Stage at every iteration
        s.add_tasks(Task(from_dict=task_dict))

    print("Adding shared tasks. Total: {}".format(len(s.tasks)))

Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_ENTK_VERBOSE=DEBUG``.

A look at the complete code in this section:

.. literalinclude:: ../../examples/user_guide/add_tasks.py
