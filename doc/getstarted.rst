.. _getstarted:

***************
Getting Started
***************

In this section we will run you through the basics building blocks of the  API.
We will develop an example application, starting from a singe MD task, to a bag
of MD tasks, to a Pipeline of MD tasks to a  complex  Simulation-Analysis loop.

There are a few key terms that you should try to understand while reading through
this section, like `application logic`, `application workload`, `pattern`,
`kernel` and `execution context`.
The Basics
==========

The basic skeleton of an Ensemble MD Toolkit application consists of three components: a
`pattern class` in which the application logic is defined `application kernels`
which define the application workload and an `execution environment` which
represents the  cluster / resource on which the application workload is
executed.

Let's start with the simplest-possible Ensemble MD Toolkit application to see how these
parts link together:

.. code-block:: python
   :linenos:
   :emphasize-lines: 6,11,18,27


   from radical.ensemblemd import Kernel
   from radical.ensemblemd import Pipeline
   from radical.ensemblemd import EnsemblemdError
   from radical.ensemblemd import SingleClusterEnvironment

   class MyApp(Pipeline):
       def __init__(self, instances):
           Pipeline.__init__(self, instances)

       def step_1(self, instance):
           k = Kernel(name="misc.chksum")
           k.arguments = ["--inputfile=data.dat", "--outputfile=checksum{0}.sha1".format(instance)]
           k.upload_input_data  = "my_input_file.dat > data.dat"
           k.download_output_data = "checksum{0}.sha1".format(instance)
           return k

   if __name__ == "__main__":
       cluster = SingleClusterEnvironment(
           resource="localhost",
           cores=1,
           walltime=15,
           username=None,
           allocation=None
       )
       cluster.allocate()

       app = MyApp(instances=1)
       cluster.run(app)

In line **6**, we define the pattern class. Here we use the :class:`.Pipeline`
pattern. Pipeline is the "simplest" pattern in Ensemble MD Toolkit and desribes
a liniear concatenation of steps. Here we only define one step (``step_1``), so
the application logic is fairly simple.

In line **11**, we define the application kernel that we want to execute at
step_1 of the pipeline. This :class:`.Kernel` represents the application workload.

In line **18**, we create a new execution environment, a :class:`.SingleClusterEnvironment`.
A SingleClusterEnvironment represent a single resource on which the application
workload (the Kernels) are executed. We use ``localhost``, which means that the
application will simply get executed on the local machine. Further below, we
explain how you can use a remote HPC cluster instead.

In line 26 and **27**, we instantiate our application and pass it to the
SingleClusterEnvironment for execution.

You can try to run the example with the ``RADICAL_ENMD_VERBOSE`` environment
variable set to ``info`` to see some progress and status updates::

    RADICAL_ENMD_VERBOSE=info python myapp.py

**And that's pretty much all there is to EnselbleMD Toolkit.** The rest is
really all about defining your application logic with the different patterns
and Kernels. We will go through some examples throughout the rest of this
"Getting Started" guide.

Adding More Steps
=================
