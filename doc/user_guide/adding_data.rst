.. _adding_data:

****************
Adding Data
****************

Now that we have a basic example running, let's try to add some data to this application! Our kernel in the first stage, 
creates a file if it does not already exist and adds `Hello World` to the file. In this section, we'll try to first transfer an 
input file ("input_file.txt") from localhost, add `Hello World` to this file and transfer it back to the localhost.

You can download the complete code for this section :download:`here <scripts/add_data.py>` or find it in 
your virtualenv under ``share/radical.ensemblemd/user_guide/scripts``.

To do this, we just have to add the ``upload_input_data`` and ``download_output_data`` properties of the kernel. This is how our new kernel should look:

.. code-block:: python

	k = Kernel(name="misc.hello")
	k.upload_input_data = ['./input_file.txt']
    	k.arguments = ["--file=input_file.txt"]
    	k.download_output_data = ['./input_file.txt']

It is also possible to rename the files while staging them! Let's rename "input_file.txt" to "temp.txt". The local file still is named "input_file.txt", but the same file on remote will be called "temp.txt". We will also name the downloaded file to "output_file.txt". Let's look at how we can do that:

.. code-block:: python

	k = Kernel(name="misc.hello")
	k.upload_input_data = ['./input_file.txt > temp.txt']
    	k.arguments = ["--file=temp.txt"]
    	k.download_output_data = ['./temp.txt > output_file.txt']

We can simply rename files by using the '>' operator. The convention is as follows:

::

	['old_name > new_name']

All the data staging properties of the Kernel can be found in the API discussion :ref:`here <kern_api>`.

To run the script, simply execute the following from the command line:

::

     RADICAL_ENTK_VERBOSE=REPORT python add_data.py


You can generate a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.

A look at the complete code in this section:

.. literalinclude:: scripts/add_data.py
