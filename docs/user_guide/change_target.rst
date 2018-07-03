.. _uguide_change_target:


***********************
Changing Target Machine
***********************

All of our examples so far have been run locally. Its time to run something on 
a HPC! One of the features of Ensemble Toolkit is that you can submit tasks on 
another machine remotely from your local machine. But this has some requirements, 
you need to have passwordless ssh or gsissh access to the target machine. If you
don't have such access, we discuss setup :ref:`here <ssh_gsissh_setup>`. You 
also need to confirm that RP and Ensemble Toolkit are supported on this machine.
A list of supported machines and mechanism to get support for new machines is 
discussed :ref:`here <entk>`.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <app_model>` and to have read through 
            the :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.


Once you have passwordless access to another machine, switching from one target machine to another is quite simple.
We simply re-describe the resource dictionary that is used to create the Resource Manager. For example, in order to
run on the XSEDE Stampede cluster, we describe the resource dictionary as follows:

.. literalinclude:: ../../examples/user_guide/change_target.py
    :language: python
    :lines: 63-71
    :dedent: 4


You can download the complete code discussed in this section :download:`here 
<../../examples/user_guide/change_target.py>` or find it in your virtualenv under 
``share/radical.entk/user_guide/scripts``.

To run the script, simply execute the following from the command line:

.. tip:: For the purposes of this user guide, we have a MongoDB setup to use. Please run the following command to use 
        it::

            export RADICAL_PILOT_DBURL="mongodb://user:user@ds247688.mlab.com:47688/entk-docs"

.. code-block:: bash

    python change_target.py


Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_ENTK_VERBOSE=DEBUG``.

A look at the complete code in this section:

.. literalinclude:: ../../examples/user_guide/change_target.py