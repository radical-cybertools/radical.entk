.. _uguide_add_pipelines:


****************
Adding Pipelines
****************

In this section, we will take a look at how we can add more pipelines to our script from the :ref:`Adding Stages <uguide_add_stages>` section.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <overview>` and to have read through the :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.

You can download the complete code discussed in this section :download:`here <../../examples/user_guide/add_pipelines.py>` or find it in 
your virtualenv under ``share/radical.entk/user_guide/scripts``.
        

Below, you can see the code snippet that shows how you can create a workflow with two Pipelines. You simple create more 
another Pipeline objects, populate them with Stages and Tasks and create the workflow as a set of two Pipelines 
and assign them to the Application Manager. 

.. literalinclude:: ../../examples/user_guide/add_pipelines.py
    :language: python
    :lines: 43-44, 68
    :linenos:

To keep the script shorter, we created a function that creates, populates and returns a Pipeline. The code snippet 
of this function is as follows.

.. literalinclude:: ../../examples/user_guide/add_pipelines.py
    :language: python
    :lines: 11-37
    :linenos:

To run the script, simply execute the following from the command line:

.. tip:: For the purposes of this user guide, we have a MongoDB setup to use. Please run the following command to use it
        ``export RADICAL_PILOT_DBURL="mongodb://138.201.86.166:27017/ee_exp_4c"``

.. code-block:: bash

    python add_pipelines.py


Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_ENTK_VERBOSE=DEBUG``.

A look at the complete code in this section:

.. literalinclude:: ../../examples/user_guide/add_pipelines.py