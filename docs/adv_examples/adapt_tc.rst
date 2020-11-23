.. _adapt_tc:

*********************************
Adaptive applications: Task-count
*********************************

We encourage you to take a look at the Pipeline of Ensembles example on the next page and compare it with the above
pattern.

.. note:: The reader is assumed to be familiar with the :ref:`PST Model <app_model>` and to have read through the :ref:`introduction` of Ensemble Toolkit.

.. note:: This chapter assumes that you have successfully installed Ensemble Toolkit, if not see :ref:`Installation`.


You can download the complete code discussed in this section :download:`here <../../examples/advanced/adapt_tc.py>` or
find it in your virtualenv under ``share/radical.entk/advanced/scripts``.

For any adaptive capability within a Pipeline, we need to use the post
execution property of a Stage object. Decisions can only be performed once all
tasks of a Stage reached a final state (running tasks cannot be interrupted by
design).  The post execution property of a Stage requires a callable function
that can influence the next stages of a workflow.

.. code-block:: python

        CUR_NEW_STAGE=0
        MAX_NEW_STAGE=4

        def func_post():
            if CUR_NEW_STAGE <= MAX_NEW_STAGE:
                ...

        s = Stage()
        s.post_exec = func_post



In the following example, we initially create 1 Pipeline with one Stage. There
are 10 tasks in the first Stage that each runs 'sleep 30'. After the Stage is
DONE (i.e. all tasks in the Stage have completed execution), a condition is
evaluated that checks whether the number of new stages added is less than 4. If
yes, we add a new Stage with similar tasks as before to the Pipeline. If
4 stages have already been added, no more stages are added.



.. code-block:: bash

    python adapt_tc.py

Let's take a look at the complete code in the example. You can generate a more
verbose output by setting the environment variable
``RADICAL_ENTK_VERBOSE=DEBUG``.

A look at the complete code in this section:

.. literalinclude:: ../../examples/advanced/adapt_tc.py

