.. _adding_instances:

****************
Adding Instances
****************

So far in our example, we had only 1 instance of the stage, i.e. a Bag of Tasks with a bag size of 1. Let's now increase the bag size to 16, keeping the workload the same as in the previous examples. This change is trivial: we simply specify the bag size as the number of instances during pattern object creation:

.. code-block:: python

	app = MyApp(stages=1,instances=16)

You can download the entire script for this section :download:`here <scripts/add_instances.py>` or find it in 
your virtualenv under ``share/radical.ensemblemd/user_guide/scripts``.

So now we will have 16 instances of stage_1 executed. Two things to note:

* In the resource handle, we have acquired only 1 core. So these 16 instances will execute one at a time. If we had 2 cores, there will be 2 instances executed concurrently. With 16 cores, all instances execute concurrently. Play around with the number of cores and see how the runtime of the script varies!

* The output files of all instances are called ``output.txt``, they will overwrite existing files. To differentiate each of the output files, we can simply use the instance as an index in the filename.

Let's change the name of the 'staged out' file by appending to its name the instance number:

.. code-block:: python

	k.download_output_data = ['./temp.txt > output_file_{0}.txt'.format(instance)]


To run the script, simply execute the following from command line:

::

	RADICAL_ENTK_VERBOSE=REPORT python add_instances.py


You can generate a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.

As a result of the execution we will obtain 16 different output files. 


A look at the complete code in this section:

.. literalinclude:: scripts/add_instances.py
