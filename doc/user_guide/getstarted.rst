.. _getstarted:

***************
Getting Started
***************

In this section we will run you through the basic building blocks of the  API.
We will develop an example application, starting from a singe task, to a Bag
of Tasks.

.. note:: The reader is assumed to be familiar with the general Ensemble MD concepts as described in :ref:`introduction` and :ref:`overview`.

.. note:: This chapter assumes that you have successfully installed Ensemble MD, and also configured access to the resources you intent to use for the examples (see chapter :ref:`Installation`).

You can download the complete code discussed in this section :download:`here <scripts/get_started.py>` or find it in 
your virtualenv under ``share/radical.ensemblemd/user_guide/scripts``.

Importing components from the Ensemble Toolkit Module
===========================================================

To create any application using Ensemble Toolkit, you need to import three modules: Kernel, Pattern and Resource Handle. The Pattern module depends on the application requirements. We have already discussed these components in the earlier sections. The ``EnsemblemdError`` module is imported for proper reporting of any errors.

.. literalinclude:: scripts/get_started.py
	:language: python
	:lines: 5-8		


Creating a Resource Handle
================================

We create a resource handle in order to get access to a machine and acquire resources on that machine. In the following snippet of code, we create an execution context of ``SingleClusterEnvironment`` type which gives access to a single machine. The resource handle targets the local machine, requesting 1 core for a period of 15 mins. The 'username' parameter must be specified if username on a target machine differs from the local machine. 'project' parameter corresponds to an allocation number on a target system (can be unspecified for localhost). The 'database_url' parameter specifies the mongodb instance to be used.

.. literalinclude:: scripts/get_started.py
	:language: python
	:dedent: 2
	:lines: 40-50

Once created, you can now perform the following operations:

* allocate(): allocates the resources on the HPC cluster.
* run(pattern): takes a execution pattern instance and executes it on the HPC cluster.
* deallocate(): terminates the job running on the HPC cluster

Creating the Execution Pattern class
========================================

Next, we create an execution pattern class. The pattern class needs to import the specific pattern the application requires. In this case, we import the BagofTasks class. We simply create our own class ``MyApp`` that is of the BagofTasks pattern type.

.. literalinclude:: scripts/get_started.py
	:language: python
	:lines: 10-13

Once the class type is defined, we can now define ``stages`` to this pattern. Each stage of the pattern can have different workloads. We now instantiate this class,

.. code-block:: python

	app = MyApp(stages=1, instances=1)

We have created an instance of the class with 1 stage and 1 instance. The instance here refers to the number of 
instances of the stage to be executed (in other words the number of tasks with the same kernel as in a particular stage).

A complete look at our  pattern class,

.. code-block:: python

	class MyApp(PoE):
		def __init__(self, stages, instances):
			PoE.__init__(self, stages, instances)


		def stage_1(self,instance):
			<define what to do> 


	app = MyApp(stages=1, instances=1)



Using the Kernel Plugin 
===========================

We have now designed the application class completely. We can define what the first stage of the application 
needs to execute. We use the kernels to define ``what to do`` in the first stage.

.. code-block:: python
		
	k = Kernel(name="misc.hello")
	k.arguments = ["--file=output.txt"]


We use the kernel ``misc.hello`` already predefined in EnsembleMD. This kernel creates a file (if it does not exist already) with the name as defined in the argument and prints a **Hello World** within it. 


By now we have seen how to create an resource handle, define a pattern and add a kernel to the pattern. We specify the pattern to be executed on the resources by:

.. code-block:: python
		
	cluster.run(myapp)
	cluster.deallocate()

In the code snippet above, we also deallocate the resources once the application has finished executing.


To run the script, simply execute the following from the command line:

::

	RADICAL_ENTK_VERBOSE=REPORT python get_started.py


And that's it! That's all the steps of the pattern. Let's take a look at the complete code in the example. You can generate
a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.

A look at the complete code in this section:

.. literalinclude:: scripts/get_started.py
