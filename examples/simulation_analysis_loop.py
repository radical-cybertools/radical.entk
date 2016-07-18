#!/usr/bin/env python


__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Simulation-Analysis Example (generic)"


import sys
import os
import json

from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import ResourceHandle

# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
	os.environ['RADICAL_ENTK_VERBOSE'] = 'REPORT'


# ------------------------------------------------------------------------------
#
class RandomSA(SimulationAnalysisLoop):
	"""RandomSA implements the simulation-analysis loop described above. It
	   inherits from radical.ensemblemd.SimulationAnalysisLoop, the abstract
	   base class for all Simulation-Analysis applications.
	"""
	def __init__(self, maxiterations, simulation_instances=1, analysis_instances=1):
		SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)

	def pre_loop(self):
		"""pre_loop is executed before the main simulation-analysis loop is
		   started. In this example we create an initial 1 kB random ASCII file
		   that we use as the reference for all analysis stages.
		"""
		k = Kernel(name="misc.mkfile")
		k.arguments = ["--size=1000", "--filename=reference.dat"]
		k.upload_input_data = ['levenshtein.py']
		return k

	def simulation_stage(self, iteration, instance):
		"""The simulation stage generates a 1 kB file containing random ASCII
		   characters that is compared against the 'reference' file in the
		   subsequent analysis stage.
		"""
		k = Kernel(name="misc.mkfile")
		k.arguments = ["--size=1000", "--filename=simulation-{0}-{1}.dat".format(iteration, instance)]
		return k

	def analysis_stage(self, iteration, instance):
		"""In the analysis stage, we take the previously generated simulation
		   output and perform a Levenshtein distance calculation between it
		   and the 'reference' file.

		   ..note:: The placeholder ``$PRE_LOOP`` used in ``link_input_data`` is
					a reference to the working directory of pre_loop.
					The placeholder ``$PREV_SIMULATION`` used in ``link_input_data``
					is a reference to the working directory of the previous
					simulation stage.

					It is also possible to reference a specific
					simulation stage using ``$SIMULATION_N`` or all simulations
					via ``$SIMULATIONS``. Analogous placeholders exist for
					``ANALYSIS``.
		"""
		input_filename  = "simulation-{0}-{1}.dat".format(iteration, instance)
		output_filename = "analysis-{0}-{1}.dat".format(iteration, instance)

		k = Kernel(name="misc.levenshtein")
		k.link_input_data      = ["$PRE_LOOP/reference.dat", "$SIMULATION_ITERATION_{1}_INSTANCE_{2}/{0}".format(input_filename,iteration,instance),"$PRE_LOOP/levenshtein.py"]
		k.arguments            = ["--inputfile1=reference.dat",
								  "--inputfile2={0}".format(input_filename),
								  "--outputfile={0}".format(output_filename)]
		k.download_output_data = output_filename
		return k

	def post_loop(self):
		# post_loop is executed after the main simulation-analysis loop has
		# finished. In this example we don't do anything here.
		pass


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

		# We set both the the simulation and the analysis stage 'instances' to 16.
		# This means that 16 instances of the simulation stage and 16 instances of
		# the analysis stage are executed every iteration.
		randomsa = RandomSA(maxiterations=1, simulation_instances=16, analysis_instances=16)

		cluster.run(randomsa)
	
	except EnsemblemdError, er:

		print "Ensemble MD Toolkit Error: {0}".format(str(er))
		raise # Just raise the execption again to get the backtrace

	try:
		cluster.deallocate()
	except:
		pass


	# After execution has finished, we print some statistical information
	# extracted from the analysis results that were transferred back.
	for it in range(1, randomsa.iterations+1):
		print "\nIteration {0}".format(it)
		ldists = []
		for an in range(1, randomsa.analysis_instances+1):
			ldists.append(int(open("analysis-{0}-{1}.dat".format(it, an), "r").readline()))
		print "   * Levenshtein Distances: {0}".format(ldists)
		print "   * Mean Levenshtein Distance: {0}".format(sum(ldists) / len(ldists))