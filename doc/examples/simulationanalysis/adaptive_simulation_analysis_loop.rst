.. _adaptive_simulation_analysis_loop:

************************************************************
Adaptive Simulation Analysis Loop Example 
************************************************************

In the previous examples, the number of simulations is constant in every iteration. In
this example, we use the ``SimulationAnalysisLoop`` pattern in which the number of
simulations changes every iteration. We discuss two examples, 

* analysis produces the number of simulations for the next iteration 
* analysis produces a bunch of output and a script is used to extract the number of simulations for the next iteration.

.. figure:: ../../images/simulation_analysis_pattern.*
	 :width: 300pt
	 :align: center
	 :alt: Simulation-Analysis Pattern

	 Fig.: `The Simulation-Analysis Pattern.`

Run adaptive simulation analysis loop
=====================================

The main difference from the previous cases is during the pattern object creation. In many cases, the analysis stage
produces the number of simulations for the next iteration. If the analysis prints out **ONLY** the number of 
simulations and no other message, it is sufficient to specify that the pattern is adaptive by adding the parameter 
``adaptive_simulation=True``.

.. code-block:: python

		mssa = MSSA(iterations=2, simulation_instances=16, analysis_instances=1, adaptive_simulation=True)


**Step 1:** View and download the example sources :ref:`below <example_adaptive_simulation_analysis_loop>`  or find it in 
your virtualenv under ``share/radical.ensemblemd/examples/adaptive_simulation_analysis_loop.py``.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``REPORT``::

		RADICAL_ENTK_VERBOSE=REPORT python adaptive_simulation_analysis_loop.py

You can generate a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.

.. note:: The following script and the script in your ``share/radical.ensemblemd/user_guide/scripts`` have some additional parsing of arguments. This is unrelated to Ensemble Toolkit.

.. _example_adaptive_simulation_analysis_loop:


Example Source
-------------------------------

:download:`Download adaptive_simulation_analysis_loop.py <../../../examples/adaptive_simulation_analysis_loop.py>` or find it in 
your virtualenv under ``share/radical.ensemblemd/examples/adaptive_simulation_analysis_loop.py``.

.. literalinclude:: ../../../examples/adaptive_simulation_analysis_loop.py
		:language: python


Run adaptive simulation analysis loop with a script
===========================================

In the previous case, we required that the analysis phase prints **ONLY** the number of simulations. But at most times, 
this might not be the case, i.e. the analysis phase outputs other messages in addition to the required number of simulations.
This requires parsing this message to extract this number. This script should be capable of accepting the output of the analysis 
stage as standard input and should output just the number of simulations. This script (absolute path) needs to be specified as a 
parameter during pattern object creation.

.. code-block:: python

		mssa = MSSA(iterations=2, simulation_instances=16, analysis_instances=1, adaptive_simulation=True, sim_extraction_script='extract.py')


**Step 1:** View and download the example sources :ref:`below <example_adaptive_simulation_analysis_loop_with_script>`  or find it in 
your virtualenv under ``share/radical.ensemblemd/examples/adaptive_simulation_analysis_loop_with_script.py``.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``REPORT``::

		RADICAL_ENTK_VERBOSE=REPORT python adaptive_simulation_analysis_loop_with_script.py

You can generate a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.


.. _example_adaptive_simulation_analysis_loop_with_script:


Example Source
-------------------------------

:download:`Download adaptive_simulation_analysis_loop_with_script.py <../../../examples/adaptive_simulation_analysis_loop_with_script.py>` or find it in 
your virtualenv under ``share/radical.ensemblemd/examples/adaptive_simulation_analysis_loop_with_script.py``.

.. literalinclude:: ../../../examples/adaptive_simulation_analysis_loop_with_script.py
		:language: python
