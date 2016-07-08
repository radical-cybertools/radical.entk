.. _data_refs:

Data Reference Mechanisms
==========================

For the Pipeline and Simulation Analysis Loop patterns we have data references which can be used to reference files in other steps 
**ONLY** for staging purposes.


Pipeline Pattern
-------------------------------

The following placeholders can be used to reference the data during staging of files
generated in previous steps and same instance:

* ``$STAGE_X`` - References the step X with the same instance number as the current instance.

Example:

.. code-block:: python

	def stage_3(self, instance):

		k1 = Kernel("misc.cat")
		k1.copy_input_data = ["$STAGE_1/file.txt > file1.txt"]
		k1.link_input_data = ["$STAGE_2/file.txt > file2.txt"]

		..
		..

This creates a copy of file.txt created in stage_1 as file1.txt and links file.txt created in stage_2 as file2.txt.


Bag of Tasks Pattern
-------------------------------

The following placeholders can be used to reference the data during staging of files
generated in previous steps and same instance:

* ``$STAGE_X`` - References the step X with the same instance number as the current instance.

Example:

.. code-block:: python

	def stage_3(self, instance):

		k1 = Kernel("misc.cat")
		k1.copy_input_data = ["$STAGE_1/file.txt > file1.txt"]
		k1.link_input_data = ["$STAGE_2/file.txt > file2.txt"]

		..
		..

This creates a copy of file.txt created in stage_1 as file1.txt and links file.txt created in stage_2 as file2.txt.


Simulation Analysis Loop Pattern
---------------------------------------------------

The following placeholders can be used to reference the data during staging of files
generated in previous steps across different instances:

* ``$PRE_LOOP`` - References the pre_loop step.
* ``$PREV_SIMULATION`` - References the previous simulation step with the same instance number.
* ``$PREV_SIMULATION_INSTANCE_Y`` - References instance Y of the previous simulation step.
* ``$SIMULATION_ITERATION_X_INSTANCE_Y`` - Refernces instance Y of the simulation step of iteration number X.
* ``$PREV_ANALYSIS`` - References the previous analysis step with the same instance number.
* ``$PREV_ANALYSIS_INSTANCE_Y`` - References instance Y of the previous analysis step.
* ``$ANALYSIS_ITERATION_X_INSTANCE_Y`` - Refernces instance Y of the analysis step of iteration number X.

For example, to reference the file ``output.dat`` in the output
directory of the previous analysis step with the same instance can be
referenced from an analysis step as:

.. code-block:: python

	def simulation_step(self, iteration, instance):
             	k = Kernel(name="kernelname")
                	k.link_input_data = ["$PREV_ANALYSIS/output.dat]
                	# Alternatively: k.copy_input_data to copy the data instead of just linking it
                	k.arguments = ["--inputfile1=output.dat"]
                	return k