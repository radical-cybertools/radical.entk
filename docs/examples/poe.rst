.. _poe:

*********************
Pipeline of Ensembles
*********************

One of the most common execution patterns consists of one Pipeline with multiple Stages where each Stage
consists of several Tasks. We call this a **Pipeline of Ensembles**. A pictorial representation of this pattern is
provided below.


.. figure:: ../figures/poe.jpg
   :width: 500pt
   :align: center
   :alt: Pipeline of Ensembles

   `Figure 1: Pipeline of Ensembles`


We encourage you to take a look at the Ensemble of Pipelines example on the next page and compare it with the above
pattern.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <app_model>` and to have read through the :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.


In the following example, we create one Pipeline with three Stages. The Task in the first Stage creates a file. There
are 10 Tasks in the second Stage that perform a character count on that file. The 10 Tasks in the third Stage perform
a checksum on the output of each Task from the second stage.

You can download the complete code discussed in this section :download:`here <../../examples/simple/poe.py>` or find it in
your virtualenv under ``share/radical.entk/simple/scripts``.

To run the script, simply execute the following from the command line:

.. tip:: For the purposes of this example, we have a MongoDB setup to use. Please run the following command to use
        it::

            export RADICAL_PILOT_DBURL="mongodb://user:user@ds247688.mlab.com:47688/entk-docs"

.. code-block:: bash

    python poe.py

Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_ENTK_VERBOSE=DEBUG``.

A look at the complete code in this section:

.. literalinclude:: ../../examples/simple/poe.py
