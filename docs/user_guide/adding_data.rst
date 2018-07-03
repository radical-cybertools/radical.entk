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

In the following example, we will create a Pipeline of two Stages each with one Task. In the first stage, we will
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


To run the script, simply execute the following from the command line:

.. tip:: For the purposes of this user guide, we have a MongoDB setup to use. Please run the following command to use 
        it::

            export RADICAL_PILOT_DBURL="mongodb://user:user@ds247688.mlab.com:47688/entk-docs"

.. code-block:: bash

    python add_data.py


Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_ENTK_VERBOSE=DEBUG``.


A look at the complete code in this section:

.. literalinclude:: ../../examples/user_guide/add_data.py
