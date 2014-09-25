#!/usr/bin/env python

""" 
This example shows how to use the EnsembleMD Toolkit ``Pipeline`` pattern
to execute a single "Bag of Ensembles". A Bag of Ensembles is modeled as a 
pipeline with just one step. The "instances" of the pipeline corresponds to the 
number of tasks in the bag.

.. figure:: images/bag_of_ensembles.*
   :width: 300pt
   :align: center
   :alt: Bag of Ensembles

   Fig.: `A Bag of Ensembles modelled as 1-step Pipeline.`

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python bag_of_ensembles.py
"""

__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Bag of Ensembles (generic)"


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
        k.download_input_data  = "http://www.cl.cam.ac.uk/~mgk25/ucs/examples/UTF-8-demo.txt"
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
            walltime=15
        )

        # Set the 'instances' of the pipeline to 32. This means that 32 instances
        # of each pipeline step are executed. 
        # 
        # Execution of the 32 pipeline instances can happen concurrently or 
        # sequentially, depending on the resources (cores) available in the 
        # SingleClusterEnvironment. 
        ccount = CalculateChecksums(instances=32)

        cluster.run(ccount)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
