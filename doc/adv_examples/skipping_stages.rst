.. _skipping_stages:

***************
Skipping stages
***************

In many applications, it may be necessary to investigate the output of task
and decide to skip certain number of the following stages. Each of the 
"stage\_" functions of a class also have a "branch\_". In this branch function,
the user may choose to investigate the output of a specific task in order to 
set what the next stage needs to be. This can be done using the "set_next_stage"
function in each of the patterns.


In order to set the next stage, the users may use the set_next_stage() in the 
branch functions. The only argument to be specified is the stage number to
jump to. It is important to note that if the target stage is less than the 
current stage the iteration number is automatically incremented.


.. note:: In EoP, the stage change is local to a single pipeline.


.. code-block:: python

    self.set_next_stage(stage)
    

A complete example using both these function is available below for both
the patterns.


Pipeline of Ensembles
---------------------

:download:`Download example: skip_stage_poe.py <../../examples/advanced/skip_stage_poe.py>`

.. literalinclude:: ../../examples/advanced/skip_stage_poe.py
    :language: python


Ensemble of Pipelines
---------------------

:download:`Download example: skip_stage_eop.py <../../examples/advanced/skip_stage_eop.py>`

.. literalinclude:: ../../examples/advanced/skip_stage_eop.py
    :language: python

