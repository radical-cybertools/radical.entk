#!/usr/bin/env python

"""
This example shows how to use the EnsembleMD Toolkit ``AllPairsPattern``
pattern to execute n!/2(n-2)! simulation permutations of a set of n elements. In
the ``element_initialization`` step, the necessary files for the all pair are
created. Each ``element_comparison`` runs a comparison between a unique coupling
of elements.


Run Locally
^^^^^^^^^^^

.. warning:: In order to run this example, you need access to a MongoDB server and
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** View and download the example sources :ref:`below <example_source_all_pairs>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python allpairs_example.py

Once the script has finished running, you should see <Need to implement it
completely to know the output files. Probably one file for everything?>the in
the same directory you launched the script in.

Run Remotely
^^^^^^^^^^^^

By default, simulation and analysis steps run on one core your local machine::

    SingleClusterEnvironment(
        resource="localhost",
        cores=1,
        walltime=30,
        username=None,
        allocation=None
    )

You can change the script to use a remote HPC cluster and increase the number
of cores to see how this affects the runtime of the script as the individual
all pair instances can run in parallel::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        allocation=None # add your allocation or project id here if required
    )

.. _example_source_all_pairs:

Example Source
^^^^^^^^^^^^^^
"""

__author__       = "Ioannis Paraskevakos <i.parask@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "All Pairs Example (generic)"

import math

from radical.ensemblemd import Kernel
from radical.ensemblemd import AllPairs
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class RandomAP(AllPairs):
    """RandomAP implements the all pairs pattern described above. It
       inherits from radical.ensemblemd.AllPairPattern, the abstract
       base class for all All Pairs applications.
    """
    def __init__(self,setelements):
        AllPairs.__init__(self, setelements)


    def element_initialization(self,element):
        """The initialization step creates the necessary files that will be
            needed for the comparison over the elements of the set.
        """

        # Creating an ASCII file by using the misc.mkfile kernel. Each file represents
        # a element of the set.
        print "Creating Element {0}".format(element)
        k = Kernel(name = "misc.mkfile")
        k.arguments = ["--size=10000", "--filename=asciifile-{0}.dat".format(element)]
        return k

    def element_comparison(self, element1, element2):
        """In the comparison, we take the previously generated files
           and perform a difference between those files. Each file coresponds to
           an elements of the set.

        """

        input_filename1 = "asciifile-{0}.dat".format(element1)
        input_filename2 = "asciifile-{0}.dat".format(element2)
        output_filename = "comparison-{0}-{1}.log".format(element1, element2)
        print "Comparing {0} with {1}. Saving result in {2}".format(input_filename1,input_filename2,output_filename)

        # Compare the previously generated files with the misc.diff kernel and
        # write the result of each comparison to a specific output file.

        k = Kernel(name="misc.diff")
        k.arguments            = ["--inputfile1={0}".format(input_filename1),
                                  "--inputfile2={0}".format(input_filename2),
                                  "--outputfile={0}".format(output_filename)]

        # I think it download the output files from the cluster to the machine
        # that run this script. Need to learn more here.

        k.download_output_data = output_filename
        return k

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
            resource="local.localhost",
            cores=1,
            walltime=30,
            username=None,
            allocation=None
        )

        # Allocate the resources.
        cluster.allocate()

        # For example the set has 5 elements.
        ElementsSet = range(1,6)
        randAP = RandomAP(setelements=ElementsSet)

        cluster.run(randAP)

        # Do not know yet what should be printed...
        print "Completed Succefully! Everything is downloaded!"

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
