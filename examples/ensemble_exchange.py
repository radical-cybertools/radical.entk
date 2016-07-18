#!/usr/bin/env python


__author__       = "Antons Treikalis <antons.treikalis@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Synchronous Replica Exchange Example with 'local' exchange (generic)."


import os
import sys
import json
import math
import time
import random
import pprint
import optparse
import datetime
from os import path
import radical.pilot

from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import ResourceHandle
from radical.ensemblemd import Replica
from radical.ensemblemd import EnsembleExchange

# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
	os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'


#-------------------------------------------------------------------------------
#

class ReplicaP(Replica):
	"""Class representing replica and it's associated data.

	This class must be extended by users implementing RE pattern for
	specific MD kernel
	"""
	def __init__(self, my_id, cores=1):
		"""Constructor

		Arguments:
		my_id - integer representing replica's id
		cores - number of cores each replica should use
		"""
		self.id = int(my_id)
		self.cores = int(cores)
		self.parameter = random.randint(300, 600)
		self.cycle = 0

		super(ReplicaP, self).__init__(my_id)

class RePattern(EnsembleExchange):
	"""In this class are specified details of RE simulation:
		- initialization of replicas
		- generation of input files
		- preparation for MD and exchange steps
		- implementation of exchange routines
	"""
	def __init__(self, workdir_local=None):
		"""Constructor
		"""
		# hardcoded name of the input file base
		self.inp_basename = "md_input"
		# number of replicas to be launched during the simulation
		self.replicas = None
		# number of cycles the simulaiton will perform
		self.nr_cycles = None

		self.workdir_local = workdir_local

		super(RePattern, self).__init__()

	# --------------------------------------------------------------------------
	#
	def initialize_replicas(self):
		"""Initializes replicas and their attributes to default values
		"""
		try:
			self.replicas+1
		except:
			print "Ensemble MD Toolkit Error: Number of replicas must be defined for pattern ReplicaExchange!"
			raise


		replicas = []
		N = self.replicas
		for k in range(N):
			r = ReplicaP(k)
			replicas.append(r)

		return replicas

	# --------------------------------------------------------------------------
	#
	def build_input_file(self, replica):
		"""Generates dummy input file

		Arguments:
		replica - object representing a given replica and it's associated \
		parameters
		"""

		file_name = self.inp_basename + "_" + \
					str(replica.id) + "_" + \
					str(replica.cycle) + ".md"

		fo = open(file_name, "wb")
		for i in range(1,500):
			fo.write(str(random.randint(i, 500) + i*2.5) + " ");
			if i % 10 == 0:
				fo.write(str("\n"));
		fo.close()

	# --------------------------------------------------------------------------
	#
	def prepare_replica_for_md(self, replica):
		"""Specifies input and output files and passes them to kernel

		Arguments:
		replica - object representing a given replica and it's associated \
		parameters
		"""
		input_name = self.inp_basename + "_" + \
					 str(replica.id) + "_" + \
					 str(replica.cycle) + ".md"
		output_name = self.inp_basename + "_" + \
					  str(replica.id) + "_" + \
					  str(replica.cycle) + ".out"

		k = Kernel(name="misc.ccount")
		k.arguments            = ["--inputfile=" + input_name, 
								  "--outputfile=" + output_name]
		k.upload_input_data      = input_name
		k.download_output_data = output_name
		k.cores = 1

		replica.cycle = replica.cycle + 1
		return k

	# --------------------------------------------------------------------------
	#
	def prepare_replica_for_exchange(self, replica):
		"""This is not used in this example, but implementation is still \
		required

		Arguments:
		replica - object representing a given replica and it's associated \
		parameters
		"""
		pass

	#---------------------------------------------------------------------------
	#
	def exchange(self, r_i, replicas, swap_matrix):
		"""Given replica r_i returns replica r_j for r_i to perform an \
		exchange with

		Arguments:
		replicas - a list of replica objects
		swap_matrix - matrix of dimension-less energies, where each column is \
		a replica and each row is a state
		"""
		return random.choice(replicas)

	#---------------------------------------------------------------------------
	#
	def get_swap_matrix(self, replicas):
		"""Creates and populates swap matrix used to determine exchange \
		probabilities

		Arguments:
		replicas - a list of replica objects
		"""
		# init matrix
		swap_matrix = [[ 0. for j in range(len(replicas))]
			for i in range(len(replicas))]

		return swap_matrix

	#---------------------------------------------------------------------------
	#
	def perform_swap(self, replica_i, replica_j):
		"""Performs an exchange of parameters

		Arguments:
		replica_i - a replica object
		replica_j - a replica object
		"""
		param_i = replica_i.parameter
		replica_i.parameter = replica_j.parameter
		replica_j.parameter = param_i

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

		workdir_local = os.getcwd()

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

		# creating RE pattern object
		re_pattern = RePattern(workdir_local)

		# set number of replicas
		re_pattern.replicas = 8
 
		# set number of cycles
		re_pattern.nr_cycles = 3

		# initializing replica objects
		replicas = re_pattern.initialize_replicas()

		re_pattern.add_replicas( replicas )

		# run RE simulation
		cluster.run(re_pattern, force_plugin="replica_exchange.static_pattern_1")

	except EnsemblemdError, er:

		print "Ensemble MD Toolkit Error: {0}".format(str(er))
		raise # Just raise the execption again to get the backtrace

	try:
		cluster.deallocate()
	except:
		pass