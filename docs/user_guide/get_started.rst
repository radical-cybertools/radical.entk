.. _uguide_get_started:


***************
Getting Started
***************

In this section we will run you through Ensemble Toolkit  API.
We will develop an example application consisting of a simple bag of Tasks.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <app_model>` and to have read through the :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.

You can download the complete code discussed in this section :download:`here <../../examples/user_guide/get_started.py>` or find it in 
your virtualenv under ``share/radical.entk/user_guide/scripts``.


Use pre-configured MongoDB
==========================

For the purposes of this user guide, we have a MongoDB setup to use. Please run the following command to use it:

.. code-block:: bash

        export RADICAL_PILOT_DBURL="mongodb://user:user@ds247688.mlab.com:47688/entk-docs"


Importing components from the Ensemble Toolkit Module
===========================================================

To create any application using Ensemble Toolkit, you need to import five modules: Pipeline, Stage, Task, AppManager, ResourceManager. We have already discussed these components in the earlier sections. 

.. literalinclude:: ../../examples/user_guide/get_started.py
    :language: python
    :lines: 1    
    :linenos:


Creating the workflow
=====================


We first create a Pipeline, Stage and Task object. Then we assign the 'executable' and 'arguments' for the Task. For
this example, we will create one Pipeline consisting of one Stage that contains one Task. 

In the below snippet, we first create a Pipeline then a Stage.

.. literalinclude:: ../../examples/user_guide/get_started.py
    :language: python
    :lines: 13-17
    :linenos:
    :lineno-start: 13

Next, we create a Task and assign its name, executable and arguments of the executable.

.. literalinclude:: ../../examples/user_guide/get_started.py
    :language: python
    :lines: 19-23
    :linenos:
    :lineno-start: 19


Now, that we have a fully described Task, a Stage and a Pipeline. We create our workflow by adding the Task to the
Stage and adding the Stage to the Pipeline.

.. literalinclude:: ../../examples/user_guide/get_started.py
    :language: python
    :lines: 25-29
    :linenos:
    :lineno-start: 25


Creating the Resource Manager
=============================


Now that our workflow is created, we need to specify where the it needs to be executed. For this example, we will 
simply execute the workflow locally. We describe a resource request for 1 core for 10 minutes on localhost, i.e. your
local machine. With this description, we create the Resource Manager object.

.. literalinclude:: ../../examples/user_guide/get_started.py
    :language: python
    :lines: 32-44
    :linenos:
    :lineno-start: 31


Creating the Application Manager
================================

Next, we create the Application Manager object and assign the workflow and the resource manager to it. Notice how we 
assign the workflow as a set of Pipelines, in this case just one Pipeline. We then run our workflow locally by calling 
the run() method of the Application Manager.


.. literalinclude:: ../../examples/user_guide/get_started.py
    :language: python
    :lines: 46-56
    :linenos:
    :lineno-start: 46

To run the script, simply execute the following from the command line:

.. code-block:: bash

    python get_started.py


And that's it! That's all the steps of the example. Let's take a look at the complete code in the example. You can generate
a more verbose output by setting the environment variable ``RADICAL_ENTK_VERBOSE=DEBUG``.

A look at the complete code in this section:

.. literalinclude:: ../../examples/user_guide/get_started.py
