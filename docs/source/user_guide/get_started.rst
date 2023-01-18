.. _uguide_get_started:


***************
Getting Started
***************

In this section we will run through the Ensemble Toolkit API. We will develop an
example application consisting of a simple bag of Tasks.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <app_model>` and to have read through the :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.

You can download the complete code discussed in this section :download:`here <../../../examples/user_guide/get_started.py>` or find it in
your virtualenv under ``share/radical.entk/user_guide/scripts``.


Importing components from the Ensemble Toolkit Module
===========================================================

To create any application using Ensemble Toolkit, you need to import five
modules: Pipeline, Stage, Task, AppManager, ResourceManager. We have already
discussed these components in the earlier sections.

.. literalinclude:: ../../../examples/user_guide/get_started.py
    :language: python
    :lines: 3
    :linenos:


Creating the workflow
=====================


We first create a Pipeline, Stage and Task object. Then we assign the
'executable' and 'arguments' for the Task. For this example, we will create one
Pipeline consisting of one Stage that contains one Task.

In the below snippet, we first create a Pipeline then a Stage.

.. literalinclude:: ../../../examples/user_guide/get_started.py
    :language: python
    :lines: 22-28
    :linenos:
    :lineno-start: 22

Next, we create a Task and assign its name, executable and arguments of the
executable.

.. literalinclude:: ../../../examples/user_guide/get_started.py
    :language: python
    :lines: 30-34
    :linenos:
    :lineno-start: 30


Now, that we have a fully described Task, a Stage and a Pipeline. We create our
workflow by adding the Task to the Stage and adding the Stage to the Pipeline.

.. literalinclude:: ../../../examples/user_guide/get_started.py
    :language: python
    :lines: 37-41
    :linenos:
    :lineno-start: 37


Creating the AppManager
=======================

Now that our workflow has been created, we need to specify where it is to be
executed. For this example, we will simply execute the workflow locally. We
create an AppManager object, describe a resource request for 1 core for 10
minutes on localhost, i.e. your local machine. We assign the resource request
description and the workflow to the AppManager and ``run`` our application.

.. literalinclude:: ../../../examples/user_guide/get_started.py
    :language: python
    :lines: 42-64
    :linenos:
    :lineno-start: 42


.. warning:: If the python version your system has by default is Anaconda python,
          please change line 51 in the above code block to

          .. code-block:: python

                'resource': 'local.localhost_anaconda',

To run the script, simply execute the following from the command line:

.. code-block:: bash

    python get_started.py


.. warning:: The first run may fail for different reasons, most of which
        related to setting up the execution environment or requesting the correct
        resources. Upon failure, Python may incorrectly raise the exception
        ``KeyboardInterrupt``. This may be confusion because it is reported even when
        no keyboard interrupt has been issued. Currently, we did not find a way to
        avoid to raise that exception.


And that's it! That's all the steps in this example. You can generate more
verbose output by setting the environment variable **`export
RADICAL_LOG_TGT=radical.log;export RADICAL_LOG_LVL=DEBUG`**.

After the execution of the example, you may want to check the output. Under your
home folder, you will find a folder named `radical.pilot.sandbox`. In that
folder, there will be a `re.session.*` folder and a `ve.local.localhost` folder.
Inside, `re.session.*`, there is a `pilot.0000` folder and in there a
`unit.000000` folder. In the unit folder, you will see several files including a
`unit.000000.out` and `unit.000000.err` files. The `unit.000000.out` holds the
messages from the standard output and `unit.000000.err` holds the messages from
standard error. The `unit.000000.out` file should have a `Hello World` message.

Let's look at the complete code for this example:

.. literalinclude:: ../../../examples/user_guide/get_started.py
