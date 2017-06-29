.. _shared_data:

************************
Working with Shared Data
************************

In many cases, there are input data that are shared between multiple tasks. It 
would not be efficient to upload this data multiple times to each task. The best 
way would be to be upload it once to shared location and have each of the tasks 
link or copy from that location. Ensemble toolkit allows you to do the same.

The data can be staged to the remote resource while creating the resource
request using the 'shared_data' property of the ResourceHandle. This data can 
be referenced using the "$SHARED" data reference. The other data reference 
mechanisms can be found `here <data_refs>`.

You can download the complete code for this section :download:`here 
<scripts/shared_data.py>` or find it in your virtualenv under 
``share/radical.ensemblemd/user_guide/scripts``.

The following is an example of how one can stage-in the shared data along with
the resource request:

.. code-block:: python

    cluster = ResourceHandle(
            resource='xsede.stampede',
            cores=32,
            walltime=30,
            database_url = 'some-url'
        )

    cluster.shared_data = [ 'my_shared_file_1.txt',
                            'my_shared_file_2.txt'
                        ]

    cluster.allocate()

Following is an example of how the '$SHARED' data reference can be used in the
kernels to access these data.

.. code-block:: python

    k = Kernel(name="misc.hello")
    k.copy_input_data = ['$SHARED/my_shared_file_1.txt > file1.txt']
    k.link_input_data = ['$SHARED/my_shared_file_2.txt > file2.txt']
    k.arguments = ["--file=file1.txt"]



To run the example script, simply execute the following from the command line:

.. code-block:: bash

    RADICAL_ENTK_VERBOSE=REPORT python shared_data.py


You can generate a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.

A look at the complete code in this section:

.. literalinclude:: scripts/shared_data.py
