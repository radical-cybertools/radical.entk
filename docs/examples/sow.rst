.. _sow:

*********************
Sequence of Workflows
*********************

Another common execution pattern consists of sequential workflows with a single or mulitple concurrent Pipelines with multiple Stages where each Stage
consists of several Tasks. We call this a **Sequence of Workflows**.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <app_model>` and to have read through the :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.

In the following example, we create 2 Pipelines, each with three Stages. For demonstration purposes, each Task does nothing but "sleep" for 3 seconds.

You can download the complete code discussed in this section :download:`here <../../examples/simple/sow.py>` or find it in
your virtualenv under ``share/radical.entk/simple/scripts``.

.. code-block:: bash

    python sow.py

Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_ENTK_VERBOSE=DEBUG``.

A look at the complete code in this section:

.. literalinclude:: ../../examples/simple/sow.py
