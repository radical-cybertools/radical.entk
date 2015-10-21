from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

#Used to register user defined kernels
from radical.ensemblemd.engine import get_engine

#Import our new kernel
from new_kernel import MyUserDefinedKernel

# Register the user-defined kernel with Ensemble MD Toolkit.
get_engine().add_kernel_plugin(MyUserDefinedKernel)


#Now carry on with your application as usual !
class Sleep(Pipeline):

    def __init__(self, instances,steps):
        Pipeline.__init__(self, instances,steps)

    def step_1(self, instance):
        """This step sleeps for 60 seconds."""

        k = Kernel(name="sleep")
        k.arguments = ["--interval=10"]
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
                walltime=15,
        	   username=None,
        	    project=None
        	)

        # Allocate the resources.
        cluster.allocate()

        # Set the 'instances' of the pipeline to 16. This means that 16 instances
        # of each pipeline step are executed.
        #
        # Execution of the 16 pipeline instances can happen concurrently or
        # sequentially, depending on the resources (cores) available in the
        # SingleClusterEnvironment.
        sleep = Sleep(steps=1,instances=16)

        cluster.run(sleep)

        cluster.deallocate()

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
