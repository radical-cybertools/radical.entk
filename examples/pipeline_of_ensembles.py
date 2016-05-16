#!/usr/bin/env python

__author__       = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__    = "Copyright 2016, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Pipeline of Ensembles Example (generic)"

import sys
import os
import json

from radical.ensemblemd import Kernel
from radical.ensemblemd import BagofTasks
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class CalculateChecksums(BagofTasks):
	"""The CalculateChecksums class implements a Bag of Tasks. Since there
		is no explicit "Bag of Tasks" pattern template, we inherit from the
		radical.ensemblemd.Pipeline pattern and define just one stage.
	"""

	def __init__(self, stages, instances):
		BagofTasks.__init__(self, stages, instances)

	def stage_1(self, instance):
		"""This stage downloads a sample UTF-8 file from a remote websever and
		   calculates the SHA1 checksum of that file. The checksum is written
		   to an output file and tranferred back to the host running this
		   script.
		"""
		k = Kernel(name="misc.chksum")
		k.arguments            = ["--inputfile=UTF-8-demo.txt", "--outputfile=checksum{0}.sha1".format(instance)]
		k.upload_input_data  = "UTF-8-demo.txt"
		k.download_output_data = "checksum{0}.sha1".format(instance)
		
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

		ccount = CalculateChecksums(stages=1,instances=16)

		os.system('wget -q -o UTF-8-demo.txt http://gist.githubusercontent.com/oleweidner/6084b9d56b04389717b9/raw/611dd0c184be5f35d75f876b13604c86c470872f/gistfile1.txt')

		cluster.run(ccount)

		# Print the checksums
		print "\nResulting checksums:"
		import glob
		for result in glob.glob("checksum*.sha1"):
			print "  * {0}".format(open(result, "r").readline().strip())

		cluster.deallocate()

	except EnsemblemdError, er:

		print "Ensemble MD Toolkit Error: {0}".format(str(er))
		raise # Just raise the execption again to get the backtrace
