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
		k.arguments = ["--file=output.txt"]
		return k

if __name__ == "__main__":


	try:

		# Create a new resource handle with one resource and a fixed
		# number of cores and runtime.
		cluster = SingleClusterEnvironment(
			resource="localhost",
			cores=1,
			walltime=15,
			username=None,
			project=None,
			database_url='mongodb://entk_user:entk_user@ds029224.mlab.com:29224/entk_doc',
			)


		# Allocate the resources.
		cluster.allocate()

		# Set the 'instances' of the BagofTasks to 1. This means that 1 instance
		# of each BagofTasks step is executed.
		app = MyApp(stages=1,instances=1)

		cluster.run(app)

		# Deallocate the resources. 
		cluster.deallocate()

	except EnsemblemdError, er:

		print "Ensemble MD Toolkit Error: {0}".format(str(er))
		raise # Just raise the execption again to get the backtrace
