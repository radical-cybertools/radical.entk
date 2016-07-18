#!/usr/bin/env python

'''
This script is an example of how the simulation adaptivity can be used. In the first iteration,
the number of simulations is the same as that set while pattern creation ('simulation_instances').

NOTE: There is an extra parameter during pattern creation, "adaptive_simulation=True". If this is
not set, every iteration will have the same number of simulations.

The analysis stage (or the last kernel of the analysis stage, if there are multiple kernels) is expected
to produce the number of simulations for the next iteration. There are two methods of extracting this 
value:

1) If the analysis kernel directly prints the number of simulations (prints ONLY the number of simulations).
Then that output is automatically returned by the pattern, assigned as the number of simulations 
for the next iteration.

2) If the analysis kernel prints messages/logs in addition to the number of simulations. The user can 
specify a script to be run (locally/client side) to extract the number of simulations from the verbose 
output. This script should be capable of accepting the output of the analysis stage as standard input
and should output just the number of simulations.

This example presents case 2.
'''

__author__       = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Multiple Simulations Instances, Single Analysis Instance Example (MSSA)"


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
class MSSA(SimulationAnalysisLoop):
	"""MSMA exemplifies how the MSMA (Multiple-Simulations / Multiple-Analsysis)
	   scheme can be implemented with the SimulationAnalysisLoop pattern.
	"""
	def __init__(self, iterations, simulation_instances, analysis_instances, adaptive_simulation, sim_extraction_script):
		SimulationAnalysisLoop.__init__(self, iterations, simulation_instances, analysis_instances, adaptive_simulation, sim_extraction_script)


	def simulation_stage(self, iteration, instance):
		"""In the simulation stage we simply create files with 1000 characters.
		"""
		k = Kernel(name="misc.mkfile")
		k.arguments = ["--size=1000", "--filename=asciifile-{0}.dat".format(instance)]
		k.download_output_data = ['asciifile-{0}.dat > iter{1}/asciifile-{0}.dat'.format(instance,iteration)]
		return [k]

	def analysis_stage(self, iteration, instance):
		""" In the analysis stage, we use the 'randval' kernel to output a random number within 
		the upperlimit. The output is simply a number (and no other messages). Hence, we do not mention
		and extraction scripts. The pattern automatically picks up the number.
		"""
		k = Kernel(name="misc.randval_2")
		k.arguments = ["--upperlimit=16"]
		return [k]


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

		# Create a new resource hande with one resource and a fixed
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

		# We set the simulation 'instances' to 16 and analysis 'instances' to 1. We set the adaptive
		# simulation to True and specify the simulation extraction script to be used.
		cur_path = os.path.dirname(os.path.abspath(__file__))
		mssa = MSSA(iterations=2, simulation_instances=16, analysis_instances=1, adaptive_simulation=True, sim_extraction_script='{0}/extract.py'.format(cur_path))

		cluster.run(mssa)

	except EnsemblemdError, er:

		print "Ensemble MD Toolkit Error: {0}".format(str(er))
		raise # Just raise the execption again to get the backtrace

	try:
		cluster.deallocate()
	except:
		pass