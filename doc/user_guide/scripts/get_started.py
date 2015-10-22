import sys
import os
import json

from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

class MyApp(Pipeline):

      def __init__(self, steps,instances):
             Pipeline.__init__(self, steps,instances)

      def step_1(self, instance):
            k = Kernel(name="misc.hello")
            k.arguments = ["--file=output.txt"]
            return k

if __name__ == "__main__":


      # use the resource specified as argument, fall back to localhost
      if   len(sys.argv)  > 2: 
            print 'Usage:\t%s [resource]\n\n' % sys.argv[0]
            sys.exit(1)
      elif len(sys.argv) == 2: 
            resource = sys.argv[1]
      else: 
            resource = 'local.localhost'

      try:

            with open('%s/config.json'%os.path.dirname(os.path.abspath(__file__))) as data_file:    
                  config = json.load(data_file)

            # Create a new static execution context with one resource and a fixed
            # number of cores and runtime.
            cluster = SingleClusterEnvironment(
                        resource=resource,
                        cores=1,
                        walltime=15,
                        #username=None,

                        project=config[resource]['project'],
                        access_schema = config[resource]['schema'],
                        queue = config[resource]['queue'],

                        database_url='mongodb://ec2-54-221-194-147.compute-1.amazonaws.com:24242',
                        database_name='myexps',
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
