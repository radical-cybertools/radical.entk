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

    def __init__(self, steps, instances):
        Pipeline.__init__(self, steps, instances)

    def step_1(self, instance):
        """The first step of the pipeline creates a 1 MB ASCI file.
        """
        k = Kernel(name="misc.mkfile")
        k.arguments = ["--size=1000000", "--filename=asciifile-{0}.dat".format(instance)]
        return k

    def step_2(self, instance):
        """The second step of the pipeline does a character frequency analysis
           on the file generated the first step. 
        """

        k = Kernel(name="misc.ccount")
        k.arguments            = ["--inputfile=asciifile-{0}.dat".format(instance), "--outputfile=cfreqs-{0}.dat".format(instance)]
        k.link_input_data      = "$STEP_1/asciifile-{0}.dat".format(instance)
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
        ccount = CharCount(steps=3,instances=16)

        cluster.run(ccount)

        # Print the checksums
        print "\nResulting checksums:"
        import glob
        for result in glob.glob("cfreqs-*.sha1"):
            print "  * {0}".format(open(result, "r").readline().strip())

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace