.. _uguide_get_started:


***************
Getting Started
***************

In this section we will run through the Ensemble Toolkit API.
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
    :lines: 20-24
    :linenos:
    :lineno-start: 20

Next, we create a Task and assign its name, executable and arguments of the executable.

.. literalinclude:: ../../examples/user_guide/get_started.py
    :language: python
    :lines: 26-30
    :linenos:
    :lineno-start: 26


Now, that we have a fully described Task, a Stage and a Pipeline. We create our workflow by adding the Task to the
Stage and adding the Stage to the Pipeline.

.. literalinclude:: ../../examples/user_guide/get_started.py
    :language: python
    :lines: 32-36
    :linenos:
    :lineno-start: 32


Creating the AppManager 
=======================

Now that our workflow has been created, we need to specify where it is to be executed. For this example, we will 
simply execute the workflow locally. We create an AppManager object, describe a resource request for 1 core for 10 
minutes on localhost, i.e. your local machine. We assign the resource request description and the workflow to the
AppManager and ``run`` our application.

.. literalinclude:: ../../examples/user_guide/get_started.py
    :language: python
    :lines: 38-59
    :linenos:
    :lineno-start: 38



To run the script, simply execute the following from the command line:

.. code-block:: bash

    python get_started.py


And that's it! That's all the steps in this example. You can generate more verbose output
by setting the environment variable ``RADICAL_ENTK_VERBOSE=DEBUG``.

Let's look at the complete code for this example:

.. literalinclude:: ../../examples/user_guide/get_started.py
