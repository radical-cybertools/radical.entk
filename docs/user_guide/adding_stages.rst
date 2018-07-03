.. _uguide_add_stages:


*************
Adding Stages
*************

In this section, we will take a look at how we can add more tasks to our script from the 
:ref:`Adding Tasks <uguide_add_tasks>` section.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <app_model>` and to have read through the 
    :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.

You can download the complete code discussed in this section :download:`here <../../examples/user_guide/add_stages.py>` 
or find it in your virtualenv under ``share/radical.entk/user_guide/scripts``.

        
Below, you can see the code snippet that shows how you can add more Stages to a Pipeline. You simple create more Stage 
objects, populate them with Tasks and **add** them to the Pipeline using the **add_stage()** method.

.. literalinclude:: ../../examples/user_guide/add_stages.py
    :language: python
    :lines: 20-58
    :dedent: 4

To run the script, simply execute the following from the command line:

.. tip:: For the purposes of this user guide, we have a MongoDB setup to use. Please run the following command to use 
        it::

            export RADICAL_PILOT_DBURL="mongodb://user:user@ds247688.mlab.com:47688/entk-docs"

.. code-block:: bash

    python add_stages.py


Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_ENTK_VERBOSE=DEBUG``.

A look at the complete code in this section:

.. literalinclude:: ../../examples/user_guide/add_stages.py