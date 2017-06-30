.. _looping:

*******
Looping
*******

Many applications consist of multiple iterations of the patterns. So, after the
last stage of the pattern, the first stage is required to be executed again. It
is possible to do the same in EnTK using the set_next_stage() function by 
setting the target stage to be 1.

It is important to note that if the target stage is less than the current stage
the iteration number is automatically incremented.

.. code-block:: python

    self.set_next_stage(stage=1)

A complete example using both these function is available below for both
the patterns.


Pipeline of Ensembles
---------------------

:download:`Download example: looping_poe.py <../../examples/advanced/looping_poe.py>`

.. literalinclude:: ../../examples/advanced/looping_poe.py
    :language: python


Ensemble of Pipelines
---------------------

:download:`Download example: looping_eop.py <../../examples/advanced/looping_eop.py>`

.. literalinclude:: ../../examples/advanced/looping_eop.py
    :language: python
