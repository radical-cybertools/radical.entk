.. _changing_remote:

*************************************
Changing Target Machine
*************************************


In most of the examples in EnsembleMD, you will see that the script is pre-configured to run locally on your machine. In order to change the target, you would need to edit the resource handle (SingleClusterEnvironment) alone.

.. note:: In order to use a specific machine, it either needs to be already support by RADICAL Pilot (:ref:`check here <chapter_resources>`)  or you would need to create a configuration file (:ref:`check here<custom_res>`).


You may find the script containing the resource handle as follows:

.. code-block:: python

	cluster = SingleClusterEnvironment(
                        resource="localhost",
                        cores=1,
                        walltime=15,
                        username=None,
                        project=None,
                        database_name="mongod:mymongodburl" #Can be skipped if using ENV variable
                    )

To change the target to a machine with a label ``XYZ.new_machine``, the resource handle may look like:

.. code-block:: python

	cluster = SingleClusterEnvironment(
                        resource="XYZ.new_machine",
                        cores=<specify the no. of cores to be acquired>,
                        walltime=<specify the duration of the acquisition>,
                        username=<set username to access new_machine>,
                        project=<set allocation id for new_machine>,
                        database_name="mongod:mymongodburl" #Can be skipped if using ENV variable
                    )

