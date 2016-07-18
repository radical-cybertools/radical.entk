#!/usr/bin/env python

__author__       = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Multiple Simulations Instances, Multiple Analysis Instances Example (MSMA)"

import sys
import os
import json


from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import ResourceHandle

# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
	os.environ['RADICAL_ENTK_VERBOSE'] = 'REPORT'


# ------------------------------------------------------------------------------
#
class MSMA(SimulationAnalysisLoop):
	"""MSMA exemplifies how the MSMA (Multiple-Simulations / Multiple-Analsysis)
	   scheme can be implemented with the SimulationAnalysisLoop pattern.
	"""
	def __init__(self, iterations, simulation_instances, analysis_instances):
		SimulationAnalysisLoop.__init__(self, iterations, simulation_instances, analysis_instances)


	def simulation_stage(self, iteration, instance):
		"""In the simulation stage we
		"""
		k = Kernel(name="misc.mkfile")
		k.arguments = ["--size=1000", "--filename=asciifile.dat"]
		return k

	def analysis_stage(self, iteration, instance):
		"""In the analysis stage we use the ``$PREV_SIMULATION`` data reference
		   to refer to the previous simulation. The same
		   instance is picked implicitly, i.e., if this is instance 5, the
		   previous simulation with instance 5 is referenced.
		"""
		k = Kernel(name="misc.ccount")
		k.arguments            = ["--inputfile=asciifile.dat", "--outputfile=cfreqs.dat"]
		k.link_input_data      = "$PREV_SIMULATION/asciifile.dat".format(instance=instance)
		k.download_output_data = "cfreqs.dat > cfreqs-{iteration}-{instance}.dat".format(instance=instance, iteration=iteration)
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

		# Create a new resource handle with one resource and a fixed
		# number of cores and runtime.
		cluster = ResourceHandle(
				resource=resource,
				cores=config[resource]["cores"],
				walltime=15,
				#username=None,

				project=config[resource]['project'],
				access_schema = config[resource]['schema'],
				queue = config[resource]['queue'],
				database_url='mongodb://rp:rp@ds015335.mlab.com:15335/rp',
			)

		# Allocate the resources. 
		cluster.allocate()

		# We set both the the simulation and the analysis stage 'instances' to 8.
		msma = MSMA(iterations=2, simulation_instances=8, analysis_instances=8)

		cluster.run(msma)

	except EnsemblemdError, er:

		print "Ensemble MD Toolkit Error: {0}".format(str(er))
		raise # Just raise the execption again to get the backtrace

	try:
		cluster.deallocate()
	except:
		pass