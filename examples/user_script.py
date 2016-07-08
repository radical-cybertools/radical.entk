import sys
import os
import json

from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

#Used to register user defined kernels
from radical.ensemblemd.engine import get_engine

#Import our new kernel
#from dev_kernel import MyUserDefinedKernel     #Solution
from user_kernel import MyUserDefinedKernel

# Register the user-defined kernel with Ensemble MD Toolkit.
get_engine().add_kernel_plugin(MyUserDefinedKernel)


#Now carry on with your application as usual !
class Sleep(Pipeline):

	def __init__(self, steps, instances):
		Pipeline.__init__(self, steps, instances)

	def step_1(self, instance):
		"""Run AMBER MD Simulations"""

		k = Kernel(name="amber")
		k.arguments = [  "--minfile=min.in",
									"--topfile=penta.top",
									"--crdfile=penta.crd",
									"--output=md.crd"]
		k.upload_input_data = ['amber_input/min.in','amber_input/penta.top','amber_input/penta.crd']
		k.download_output_data = ['md.crd > amber_output/md_{0}.crd'.format(instance)]
		return k


# ------------------------------------------------------------------------------
#
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

				database_url='mongodb://extasy:extasyproject@extasy-db.epcc.ed.ac.uk/radicalpilot',
				#database_name='myexps',
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
