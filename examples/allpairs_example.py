#!/usr/bin/env python

"""
This example shows how to use the EnsembleMD Toolkit ``AllPairs`` pattern to
execute n(n-1)/2 simulation permutations of a set of n elements. In
the ``element_initialization`` step, the necessary files for the "all pair" comparison are
created. This step uses the ``misc.mkfile`` kernel to create random ASCII files.
Those files represent the elements of the set upon which the ``AllPairs`` pattern
will be executed. After the creation an ``element_comparison`` will be executed
for each unique pair of files (elements of the set). The ``element_comparison``
here executes the ``misc.diff`` kernel which returns the number of different lines
between the files. In the end the result files are downloaded to the folder from
which the script was run.

Run Locally
^^^^^^^^^^^

.. warning:: In order to run this example, you need access to a MongoDB server and
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** View and download the example sources :ref:`below <example_source_all_pairs_pattern>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python allpairs_example.py

Once the script has finished running, you should see ``comparison-x-y.log`` files
in the same directory you launched the script in. This log files contain the number
of different lines between the compared files.

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
    def __init__(self,set1elements, windowsize1, set2elements=None, windowsize2=None):
        AllPairs.__init__(self, set1elements=set1elements,windowsize1=windowsize1,
            set2elements=set2elements,windowsize2=windowsize2)


    def set1element_initialization(self,element):
        """The initialization step creates the necessary files that will be
            needed for the comparison over the elements of the set.
        """

        # Creating an ASCII file by using the misc.mkfile kernel. Each file represents
        # a element of the set.
        print "Creating Element {0}".format(element)
        k = Kernel(name = "misc.mkfile")
        k.arguments = ["--size=10000", "--filename=asciifile_{0}.dat".format(element)]
        return k

    def set2element_initialization(self,element):
        """The initialization step creates the necessary files that will be
            needed for the comparison over the elements of the set.
        """

        # Creating an ASCII file by using the misc.mkfile kernel. Each file represents
        # a element of the set.
        print "Creating Element {0}".format(element)
        k = Kernel(name = "misc.mkfile")
        k.arguments = ["--size=10000", "--filename=newfile_{0}.dat".format(element)]
        return k

    def element_comparison(self, elements1, elements2):
        """In the comparison, we take the previously generated files
           and perform a difference between those files. Each file coresponds to
           an elements of the set.
        """

        input_filename1 = "asciifile_{0}.dat".format(elements1[0])
        input_filename2 = "newfile_{0}.dat".format(elements2[0])
        output_filename = "comparison_{0}_{1}.log".format(elements1[0], elements2[0])
        print "Comparing {0} with {1}. Saving result in {2}".format(input_filename1,input_filename2,output_filename)

        # Compare the previously generated files with the misc.diff kernel and
        # write the result of each comparison to a specific output file.

        k = Kernel(name="misc.diff")
        k.arguments            = ["--inputfile1={0}".format(input_filename1),
                                  "--inputfile2={0}".format(input_filename2),
                                  "--outputfile={0}".format(output_filename)]

        # Download the result files.
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
            walltime=30
        )

        # Allocate the resources.
        cluster.allocate()

        # For example the set has 5 elements.
        ElementsSet1 = range(1,6)
        randAP = RandomAP(set1elements=ElementsSet1,windowsize1=1)

        cluster.run(randAP)

        print "Pattern Execution Completed Successfully! Results are downloaded!"


    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace