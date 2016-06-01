.. _adding_steps:

****************
Adding Stages
****************

We have looked at creating a simple bag of tasks application, adding data movement, creating BoT with varying bag size. What's next? Let's add more stages to the pattern!

You can download the entire script for this section :download:`here <scripts/add_stages.py>` or find it in 
your virtualenv under ``share/radical.ensemblemd/user_guide/scripts``.

In this section, we will modify the example from the previous section by adding another stage, which concatenates two files and transfers the output back to the localhost. One of the files is staged from localhost and the other file is the file from stage_1.The kernel for concatenation of files is already a part of Ensemble MD. This is how our stage_2 looks:

.. code-block:: python

	k = Kernel(name="misc.cat")
	k.upload_input_data = ['./input_file_2.txt > file2.txt']
	k.copy_input_data = ['$STAGE_1/temp.txt > file1.txt']
    	k.arguments = ["--file1=file1.txt","--file2=file2.txt"]
    	k.download_output_data = ['./file1.txt > output_file.txt']

Also need to specify the number of stages during pattern object creation:

.. code -block:: python

	app = MyApp(stages=2,instances=16)

A few points to note:

* We upload a secondary file in stage_2 called input_file_2.txt.
* As part of the pattern, the data at each stage can be referred by ``$STAGE_*``. So in stage_2 we refer to the output of stage_1 by ``$STAGE_1``. We create a copy of the ``temp.txt`` created in stage_1 and rename it to file1.txt
* We concatenate file2.txt to file1.txt and download file1.txt

All the data references can be found :ref:`here <data_refs>`.


To run the script, simply execute the following from command line:

::

     RADICAL_ENTK_VERBOSE=REPORT python add_stages.py

You can generate a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.

A look at the complete code in this section:

.. literalinclude:: scripts/add_stages.py


**And that's pretty much all there is to Ensemble Toolkit.** The rest is
really all about defining your application logic with the different patterns
and Kernels. We will go through some examples throughout the rest of this
User guide.
