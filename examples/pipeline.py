#!/usr/bin/env python

"""
This example shows how to use the Ensemble MD Toolkit ``Pipeline`` pattern
to execute 16 concurrent pipeline of sequential tasks. In the first step of
each pipeline ``step_1``, a 10 MB input file is generated and filled with
ASCII charaters. In the second step ``step_2``, a character frequency analysis
if performed on this file. In the last step ``step_3``, an SHA1 checksum is
calculated for the analysis result. The results of the frequency analysis and
the SHA1 checksums are copied back to the machine on which this script runs.

.. figure:: ../images/pipeline_pattern.*
   :width: 300pt
   :align: center
   :alt: Pipeline Pattern

   Fig.: `The Pipeline Pattern.`

Run Locally
^^^^^^^^^^^

.. warning:: In order to run this example, you need access to a MongoDB server and
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** View and download the example sources :ref:`below <example_source_pipeline>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python pipeline.py

Once the script has finished running, you should see the raw data of the
character analysis step (``cfreqs-XX.dat``) and the corresponding SHA1 checksums
(``cfreqs-XX.dat.sha1``) in the same directory you launched the script in.

Run Remotely
^^^^^^^^^^^^

By default, the pipeline steps run on one core your local machine::

    SingleClusterEnvironment(
        resource="localhost",
        cores=1,
        walltime=30,
        username=None,
        project=None
    )

You can change the script to use a remote HPC cluster and increase the number
of cores to see how this affects the runtime of the script as the individual
pipeline instances can run in parallel::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        project=None # add your allocation or project id here if required
    )

.. _example_source_pipeline:

Example Source
^^^^^^^^^^^^^^
"""

__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Pipeline Example (generic)"


from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class CharCount(Pipeline):
    """The CharCount class implements a three-step pipeline. It inherits from
        radical.ensemblemd.Pipeline, the abstract base class for all pipelines.
    """

    def __init__(self, instances,steps):
        Pipeline.__init__(self, instances,steps)

    def step_1(self, instance):
        """The first step of the pipeline creates a 1 MB ASCI file.
        """
        k = Kernel(name="misc.mkfile")
        k.arguments = ["--size=1000000", "--filename=asciifile-{0}.dat".format(instance)]
        return k

    def step_2(self, instance):
        """The second step of the pipeline does a character frequency analysis
           on the file generated the first step. The result is transferred back
           to the host running this script.

           ..note:: The placeholder ``$STEP_1`` used in ``link_input_data`` is
                    a reference to the working directory of step 1. ``$STEP_``
                    can be used analogous to refernce other steps.
        """
        k = Kernel(name="misc.ccount")
        k.arguments            = ["--inputfile=asciifile-{0}.dat".format(instance), "--outputfile=cfreqs-{0}.dat".format(instance)]
        k.link_input_data      = "$STEP_1/asciifile-{0}.dat".format(instance)
        k.download_output_data = "cfreqs-{0}.dat".format(instance)
        return k

    def step_3(self, instance):
        """The third step of the pipeline creates a checksum of the output file
           of the second step. The result is transferred back to the host
           running this script.
        """
        k = Kernel(name="misc.chksum")
        k.arguments            = ["--inputfile=cfreqs-{0}.dat".format(instance), "--outputfile=cfreqs-{0}.sha1".format(instance)]
        k.link_input_data      = "$STEP_2/cfreqs-{0}.dat".format(instance)
        k.download_output_data = "cfreqs-{0}.sha1".format(instance)
        return k

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
            resource="localhost",
            cores=1,
            walltime=30,
            username=None,
            project=None,
	    database_url='mongodb://ec2-54-221-194-147.compute-1.amazonaws.com:24242',
	    database_name='myexps'
        )

        # Allocate the resources. 
        cluster.allocate()

        # Set the 'instances' of the pipeline to 16. This means that 16 instances
        # of each pipeline step are executed.
        #
        # Execution of the 16 pipeline instances can happen concurrently or
        # sequentially, depending on the resources (cores) available in the
        # SingleClusterEnvironment.
        ccount = CharCount(instances=16,steps=3)

        cluster.run(ccount)

        # Print the checksums
        #print "\nResulting checksums:"
        #import glob
        #for result in glob.glob("cfreqs-*.sha1"):
        #    print "  * {0}".format(open(result, "r").readline().strip())

        cluster.deallocate()

        df = ccount.execution_profile_dict
        df.to_pickle('exp.pkl')

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
