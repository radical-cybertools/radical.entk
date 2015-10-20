from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

class MyApp(Pipeline):

      def __init__(self, steps,instances):
             Pipeline.__init__(self, steps,instances)

      def step_1(self, instance):
            k = Kernel(name="misc.hello")
            k.upload_input_data = ['./input_file.txt > temp.txt']
            k.arguments = ["--file=temp.txt"]
            k.download_output_data = ['./temp.txt > output_file.txt']
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
                         project=None,
                         database_url='mongodb://ec2-54-221-194-147.compute-1.amazonaws.com:24242',
                         database_name='myexps'
                  )

            os.system('Welcome! > input_file.txt')

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