from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

class MyApp(Pipeline):

      def __init__(self, steps,instances):
             Pipeline.__init__(self, steps,instances)

      def step_1(self, instance):
            k = Kernel(name="misc.hello")
            k.arguments = ["--file=output.txt".format(instance)]
            return k

if __name__ == "__main__":

      try:

            # Create a new static execution context with one resource and a fixed
            # number of cores and runtime.
            cluster = SingleClusterEnvironment(
                         resource="localhost",
                        cores=1,
                        walltime=15,
                        username=None,
                        allocation=None,
                        database_name="mongod:mymongodburl"
                  )


            # Allocate the resources. 
            cluster.allocate()

            # Set the 'instances' of the pipeline to 1. This means that 1 instance
            # of each pipeline step is executed.
            app = MyApp(steps=1,instances=1)

            cluster.run(app)

            # Deallocate the resources. 
            cluster.deallocate()

      except EnsemblemdError, er:

            print "Ensemble MD Toolkit Error: {0}".format(str(er))
            raise # Just raise the execption again to get the backtrace