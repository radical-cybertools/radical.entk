#!/usr/bin/env python

"""  
This example shows how to use the EnsembleMD Toolkit ``AllPairsPattern``
pattern to execute n!/2(n-2)! simulation permutations of a set of n elements. In
the ``permuter`` step, the necessary files for the all pair are created. Each
``compare`` runs a comparison between a unique coupling of elements.


Run Locally 
^^^^^^^^^^^

.. warning:: In order to run this example, you need access to a MongoDB server and 
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly. 
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** View and download the example sources :ref:`below <example_source_all_pairs_pattern>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python simulation_analysis_loop.py

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

.. _example_source_hausdorff_example:

Example Source 
^^^^^^^^^^^^^^
"""

__author__       = "Ioannis Paraskevakos <i.parask@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "All Pairs (generic)"

import math

from radical.ensemblemd import Kernel
from radical.ensemblemd import AllPairsPattern
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class HausdorffAP(AllPairsPattern):
    """HausdorffAP implements the all pairs pattern described above. It
       inherits from radical.ensemblemd.AllPairPattern, the abstract 
       base class for all All Pairs applications.
    """
    def __init__(self,setelements):
        AllPairsPattern.__init__(self, setelements)

    def init_step(self,element):
        """The initialization step creates the necessary files that will be 
            needed for the comparison over the elements of the set.
        """

        # From what I understand the misc.<> is the script that will be run from
        # the kernel. If this is correct, I should create one for Beickstein's
        # use case based on the information from Sean's mail.
        # It would make a nice example and it can be called misc.select.
        k = Kernel(name="misc.select")
        k.arguments("--inputfile==atom_traj{0}.dcd".format(element))
        return k

    def comparison(self, element1, element2):
        """In the comparison, we take the previously generated modified trajectory
           and perform a Hausdorff distance calculation between the elements of
           the 

           ..note:: The placeholder ``$INIT_STEP`` used in ``link_input_data`` is 
                    a reference to the working directory of init_step.

        """
        input_filename1 = "traj_flat-{0}.npz.npy".format(element1)
        input_filename2 = "traj_flat-{0}.npz.npy".format(element2)
        output_filename = "comparison-{0}-{1}.dat".format(element1, element2)
    
        # From what I understand the misc.<> is the script that will be run from
        # the kernel. If this is correct, I should create one based on Beickstein's
        # script for his use case. It would make a nice example and it can be
        # called misc.hausdorff

        k = Kernel(name="misc.hausdorff")
        k.link_input_data      = ["$INIT_STEP/{0}, $INIT_STEP/{1}".format(input_filename1,input_filename2)]
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
            resource="localhost", 
            cores=1, 
            walltime=30,
            username=None,
            allocation=None
        )
        
        # For example the set has 5 elements.
        # May try it with Beickstein's use case.
        ElementsSet = range(1,6)
        hausdorff = HausdorffAP(ElementsSet)

        cluster.run(hausdorff)

        # Do not know yet what should be printed... 
        # Need to see what the use case may need.
        for it in range(1, haudorff._size+1):
            print "..."

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
