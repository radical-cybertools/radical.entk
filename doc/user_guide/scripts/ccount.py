import sys
import os
import json

from radical.ensemblemd import Kernel
from radical.ensemblemd import PoE
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import ResourceHandle

# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
	os.environ['RADICAL_ENTK_VERBOSE'] = 'REPORT'
	

class CharCount(PoE):
	"""The CharCount class implements a three-stage BagofTasks. It inherits from
		radical.ensemblemd.BagofTasks, the abstract base class for all BagofTaskss.
	"""

	def __init__(self, stages, instances):
		PoE.__init__(self, stages, instances)

	def stage_1(self, instance):
		"""The first stage of the BagofTasks creates a 1 MB ASCI file.
		"""
		k = Kernel(name="misc.mkfile")
		k.arguments = ["--size=1000000", "--filename=asciifile-{0}.dat".format(instance)]
		return k

	def stage_2(self, instance):
		"""The second stage of the BagofTasks does a character frequency analysis
		   on the file generated the first stage. 
		"""

		k = Kernel(name="misc.ccount")
		k.arguments            = ["--inputfile=asciifile-{0}.dat".format(instance), "--outputfile=cfreqs-{0}.dat".format(instance)]
		k.link_input_data      = "$STAGE_1/asciifile-{0}.dat".format(instance)
		return k

	def stage_3(self, instance):
		"""The third stage of the BagofTasks creates a checksum of the output file
		   of the second stage. The result is transferred back to the host
		   running this script.
		"""
		k = Kernel(name="misc.chksum")
		k.arguments            = ["--inputfile=cfreqs-{0}.dat".format(instance), "--outputfile=cfreqs-{0}.sha1".format(instance)]
		k.link_input_data      = "$STAGE_2/cfreqs-{0}.dat".format(instance)
		k.download_output_data = "cfreqs-{0}.sha1".format(instance)
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

		# Set the 'instances' of the BagofTasks to 16. This means that 16 instances
		# of each BagofTasks stage are executed.
		#
		# Execution of the 16 BagofTasks instances can happen concurrently or
		# sequentially, depending on the resources (cores) available in the
		# SingleClusterEnvironment.
		ccount = CharCount(stages=3,instances=16)

		cluster.run(ccount)

	except EnsemblemdError, er:

		print "Ensemble MD Toolkit Error: {0}".format(str(er))
		raise # Just raise the execption again to get the backtrace

	try:

		# Deallocate the resources. 
		cluster.deallocate()

	except:
		pass

	# Print the checksums
	print "\nResulting checksums:"
	import glob
	for result in glob.glob("cfreqs-*.sha1"):
		print "  * {0}".format(open(result, "r").readline().strip())
