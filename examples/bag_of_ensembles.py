#!/usr/bin/env python

"""
This example shows how to use the EnsembleMD Toolkit :class:`.Pipeline` pattern
to execute a single "Bag of Ensembles". A Bag of Ensembles is modeled as a
:class:`.Pipeline` with just one step. The "instances" of the  :class:`.Pipeline`
corresponds to the number of ensembles in the bag.

.. figure:: images/bag_of_ensembles.*
   :width: 300pt
   :align: center
   :alt: Bag of Ensembles

   Fig.: `A Bag of Ensembles modelled as 1-step Pipeline.`

Run Locally
^^^^^^^^^^^

.. warning:: In order to run this example, you need access to a MongoDB server and
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** View and download the example sources :ref:`below <example_source_bag_of_ensembles>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python bag_of_ensembles.py

Once the script has finished running, you should see the SHA1 checksums
genereated by the individual ensembles  (``checksumXX.sha1``) in the in the same
directory you launched the script in.

Run on a Remote Cluster
^^^^^^^^^^^^^^^^^^^^^^^

By default, this Bag of Ensembles runs on one core your local machine::

    SingleClusterEnvironment(
        resource="localhost",
        cores=1,
        walltime=30,
        username=None,
        allocation=None
    )

You can change the script to use a remote HPC cluster and increase the number
of cores to see how this affects the runtime of the script as the individual
ensembles in the bag can run in parallel::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        allocation=None # add your allocation or project id here if required
    )

.. _example_source_bag_of_ensembles:

Example Source
^^^^^^^^^^^^^^
"""

__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Bag of Ensembles Example (generic)"


from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class CalculateChecksums(Pipeline):
    """The CalculateChecksums class implements a Bag of Ensembles. Since there
        is no explicit "Bag of Ensembles" pattern template, we inherit from the
        radical.ensemblemd.Pipeline pattern and define just one step.
    """

    def __init__(self, instances):
        Pipeline.__init__(self, instances)

    def step_1(self, instance):
        """This step downloads a sample UTF-8 file from a remote websever and
           calculates the SHA1 checksum of that file. The checksum is written
           to an output file and tranferred back to the host running this
           script.
        """
        k = Kernel(name="misc.chksum")
        k.arguments            = ["--inputfile=UTF-8-demo.txt", "--outputfile=checksum{0}.sha1".format(instance)]
        k.download_input_data  = "http://gist.githubusercontent.com/oleweidner/6084b9d56b04389717b9/raw/611dd0c184be5f35d75f876b13604c86c470872f/gistfile1.txt > UTF-8-demo.txt"
        k.download_output_data = "checksum{0}.sha1".format(instance)
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
            walltime=15,
            username=None,
            allocation=None
        )

        # Set the 'instances' of the pipeline to 16. This means that 16 instances
        # of each pipeline step are executed.
        #
        # Execution of the 16 pipeline instances can happen concurrently or
        # sequentially, depending on the resources (cores) available in the
        # SingleClusterEnvironment.
        ccount = CalculateChecksums(instances=16)

        cluster.run(ccount)

        # Print the checksums
        print "\nResulting checksums:"
        import glob
        for result in glob.glob("checksum*.sha1"):
            print "  * {0}".format(open(result, "r").readline().strip())

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
