

class CharCount(radical.ensemblemd.Pipeline):

    def __init__(self, columns):
        super(Pipeline, self).__init__(width)

    def step_01(self, column):
        return Kernel(
            name="misc.mkfile", 
            args=["--size=10000000", "--filename=asciifile-%{0}.dat" % column])

    def step_02(self, column):
        return Kernel(
            name="misc.ccount", 
            args=["--inputfile=asciifile-%{0}.dat", "--outputfile=cfreqs-%{0}.dat" % column])

    def step_03(self, column):
        return Kernel(
            name="misc.ccount", 
            args=["--inputfile=cfreqs-%{0}.dat", "--outputfile=cfreqs-%{0}.sum" % column])

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

        ccount = CharCount(columns=16)

        cluster.run(ccount)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
