.. _examples:


********
Examples
********

Inside entk repository, find the examples folder. We will first work with the 
`example1.py <>`_ .

Discussion of the script
------------------------

.. code-block:: python

    from radical.entk import Pipeline, Stage, Task, AppManager

We first import the 4 objects required to create and execute our application. Our
application workflow is created using the ```Pipeline```, ```Stage``` and 
```Task``` objects. This workflow is then assigned to the ```AppManager```.

.. code-block:: python

    def create_single_task():

    t1 = Task()
    t1.name = 'simulation'
    t1.executable = ['gmx mdrun']
    t1.arguments = ['a','b','c']
    t1.copy_input_data = []
    t1.copy_output_data = []

    return t1

```create_single_task()``` is a function that simply generates a task description.
Only some of the attributes of the ```Task``` object have been used here. Please
find all the attributes in the :ref:`Task API reference <task_api>`

.. code-block:: python

    p1 = Pipeline()
    p1.name = 'p1'
    p2 = Pipeline()
    p2.name = 'p2'

In the next few lines, we create Pipeline objects and assign them names.


.. code-block:: python

    stages=3

    for cnt in range(stages):
        s = Stage()
        s.name = 's_%s'%cnt
        s.tasks = create_single_task()
        s.add_tasks(create_single_task())

        p1.add_stages(s)

Next, we add 3 Stages to Pipeline "p1". Tasks can be assigned to stages using 
``tasks`` attribute of the stage or can be added to the stages using the 
``add_tasks()`` function.

.. code-block:: python

    for cnt in range(stages-1):
        s = Stage()
        s.name = 's-%s'%cnt
        s.tasks = create_single_task()
        s.add_tasks(create_single_task())

        p2.add_stages(s)

Next, we add 2 Stages to Pipeline "p2" with 2 Tasks each.

.. code-block:: python

    appman = AppManager()
    appman.assign_workflow(set([p1,p2]))
    appman.run()

Once the application workflow has been created, it needs to be submitted for 
execution. In the above lines, we create an instance of the ``AppManager`` 
which sets up all the infrastructure for processing workflow and executing its
various tasks. Next, we assign the workflow which is a set of the two pipelines 
"p1" and "p2". We then call the ``run()`` method to initiate the execution.

Execution of the script
-----------------------

The script can be executed using the following command:

.. code-block:: bash

    RADICAL_ENTK_VERBOSE=info python example1.py

This will generate several verbose messages describing the execution. For even
more detailed output, you can set ``RADICAL_ENTK_VERBOSE`` to ``debug``.


Other examples
--------------

Two more scripts exist in the ``examples`` folder: ``seisflow.py`` and 
``anen.py``. The first script is a replica of what the script would look like
for the Seisflow use case and the latter is a replica of what the script would
look like for the Analog Ensemble use case.