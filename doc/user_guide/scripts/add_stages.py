import sys
import os
import json

from radical.ensemblemd import Kernel
from radical.ensemblemd import BagofTasks
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

class MyApp(BagofTasks):

	def __init__(self, stages,instances):
		 BagofTasks.__init__(self, stages,instances)

	def stage_1(self, instance):
		k = Kernel(name="misc.hello")
		k.upload_input_data = ['./input_file.txt > temp.txt']
		k.arguments = ["--file=temp.txt"]
		k.download_output_data = ['./temp.txt > output_file_{0}.txt'.format(instance)]
		return k

	def stage_2(self, instances):

		k = Kernel(name="misc.cat")
		k.upload_input_data = ['./output_file_2.txt > file2.txt']
		k.copy_input_data = ['$STAGE_1/temp.txt > file1.txt']
		k.arguments = ["--file1=file1.txt","--file2=file2.txt"]
		k.download_output_data = ['./file1.txt > output_file.txt']
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

			database_url='mongodb://extasy:extasyproject@extasy-db.epcc.ed.ac.uk/radicalpilot',
			#database_name='myexps',
			)

		os.system('/bin/echo Welcome! > input_file.txt')

		# Allocate the resources.
		cluster.allocate()

		# Set the 'instances' of the BagofTasks to 16. This means that 16 instances
		# of each BagofTasks step are executed.
		app = MyApp(stages=2,instances=16)

		cluster.run(app)

		# Deallocate the resources. 
		cluster.deallocate()

	except EnsemblemdError, er:

		print "Ensemble MD Toolkit Error: {0}".format(str(er))
		raise # Just raise the execption again to get the backtrace
