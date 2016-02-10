from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment


from radical.ensemblemd.engine import get_engine

### THIS EXAMPLE ASSUMES YOU HAVE SYNAPSE INSTALLED in "$HOME/ves/synapse_local".
### If your synapse is installed in a different directory, please change the value
### of the argument 'path'.


# ------------------------------------------------------------------------------
#Load synapse Kernel

from synapse.kernel import sample_Kernel
get_engine().add_kernel_plugin(sample_Kernel)

# ------------------------------------------------------------------------------
#
class Emulate(Pipeline):
    """The Emulate class implements a 1-step pipeline that uses the synapse sample kernel. 
    	It inherits from radical.ensemblemd.Pipeline, the abstract base class for all pipelines.
    """

    def __init__(self, stages,instances):
        Pipeline.__init__(self, stages,instances)

    def stage_1(self, instance):
        k = Kernel(name="synapse.sample")
        k.arguments = [	"--path=$HOME/ves/synapse_local", 
        				"--mode=sample",
        				"--flops=1000",
        				"--samples=1"
        				]
        return k


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:

        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
                        resource='local.localhost',
                        cores=1,
                        walltime=15,

        )

		# Allocate the resources. 
        cluster.allocate()

        ccount = Emulate(stages=1,instances=1)

        cluster.run(ccount)

    	#Deallocate the resource
        cluster.deallocate()

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace