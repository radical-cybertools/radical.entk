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

For any adaptive capability within a Pipeline, we need to use the post execution property of a Stage object. Decisions
can only be performed once all tasks of a Stage are completed as the concurrent tasks cannot be interrupted by design.
The post execution property of a Stage requires 3 function handles: a function that returns a boolean, a function that is
executed when the boolean result is True, and a function that is executed when the boolean result is False.

.. code-block:: python

    s = Stage()
    s.post_exec = {
                    'condition': name of boolean function,
                    'on_true': function to be executed if boolean result is True,
                    'on_false': function to be executed if boolean result is False
                }



In the following example, we initially create 1 Pipeline with one Stage. There are 10 tasks in the first Stage that each
runs 'sleep 30'. After the Stage is DONE (i.e. all tasks in the Stage have completed execution), a condition is evaluated
that checks whether the number of new stages added is less than 4. If yes, we add a new Stage with similar tasks as before
to the Pipeline. If 4 stages have already been added, no more stages are added.


To run the script, simply execute the following from the command line:

.. tip:: For the purposes of this example, we have a MongoDB setup to use. Please run the following command to use
        it::

            export RADICAL_PILOT_DBURL="mongodb://user:user@ds247688.mlab.com:47688/entk-docs"

.. code-block:: bash

    python adapt_tc.py

Let's take a look at the complete code in the example. You can generate a more verbose output by setting the environment
variable ``RADICAL_ENTK_VERBOSE=DEBUG``.

A look at the complete code in this section:

.. literalinclude:: ../../examples/advanced/adapt_tc.py
