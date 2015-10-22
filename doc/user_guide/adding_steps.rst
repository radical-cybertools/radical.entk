.. _adding_steps:

****************
Adding Steps
****************

We have looked at creating a simple pipeline application, adding data movement, creating BoT with varying bag size using the pipeline pattern. What next ? Let's add more steps to the pattern !

You can download the entire script for this section :download:`here <scripts/add_steps.py>` or find it in 
your virtualenv under ``share/radical.ensemblemd/user_guide/scripts``.

In this section, we will use the example from previous section and another step which concatenates two files and stage the output back to localhost. One of the files is staged from localhost and the other file is the file from step_1.The kernel for concatenation is already part of Ensemble MD. This is how our step_2 looks:

.. code-block:: python

	k = Kernel(name="misc.cat")
	k.upload_input_data = ['./input_file_2.txt > file2.txt']
	k.copy_input_data = ['$STEP_1/temp.txt > file1.txt']
    	k.arguments = ["--file1=file1.txt","--file2=file2.txt"]
    	k.download_output_data = ['./file1.txt > output_file.txt']

Also need to specify the number of steps during pattern object creation:

.. code -block:: python

	app = MyApp(steps=2,instances=16)

A few points to note:

* We upload a secondary file in step_2 called input_file_2.txt.
* As part of the pattern, the data at each step can be referred by ``$STEP\_*``. So in step_2 we refer to the output of step_1 by ``$STEP_1``. We create a copy of the ``temp.txt`` created in step_1 and rename it to file1.txt
* We concatenate file2.txt to file1.txt and download file1.txt

All the data references can be found :ref:`here <data_refs>`.


To run the script, simply execute the following from command line:

::

     RADICAL_ENMD_VERBOSE=REPORT python add_steps.py

Complete script:


.. literalinclude:: scripts/add_steps.py


**And that's pretty much all there is to EnselbleMD Toolkit.** The rest is
really all about defining your application logic with the different patterns
and Kernels. We will go through some examples throughout the rest of this
User guide.