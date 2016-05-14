#!/usr/bin/env python

__author__       = "Antons Treikalis <antons.treikalis@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Synchronous Replica Exchange Example with 'remote' \
					exchange (generic)."


import os
import sys
import json
import math
import time
import random
import string
import pprint
import optparse
import datetime
from os import path
import radical.pilot
from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment
from radical.ensemblemd.patterns.replica_exchange import Replica
from radical.ensemblemd.patterns.replica_exchange import ReplicaExchange

#-------------------------------------------------------------------------------
#

class ReplicaP(Replica):
	"""Class representing replica and it's associated data.

	This will have to be extended by users implementing RE pattern for
	a particular kernel and scheme
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

class RePattern(ReplicaExchange):
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
		self.sh_file = 'shared_md_input.dat'
		self.shared_urls = []
		self.shared_files = []

		super(RePattern, self).__init__()

	# --------------------------------------------------------------------------
	#
	def prepare_shared_data(self):

		fo = open(self.sh_file, "wb")
		for i in range(1,250):
			fo.write(str(random.randint(i, 500) + i*2.5) + " ");
			fo.write(str(random.choice(string.letters)) + " ");
			if i % 10 == 0:
				fo.write(str("\n"));
		fo.close()

		self.shared_files.append(self.sh_file)

		url = 'file://%s/%s' % (self.workdir_local, self.sh_file)
		self.shared_urls.append(url)

	# --------------------------------------------------------------------------
	#
	def initialize_replicas(self):
		"""Initializes replicas and their attributes to default values
		"""
		try:
			self.replicas+1
		except:
			print "Ensemble MD Toolkit Error:  Number of replicas must be \
				   defined for pattern ReplicaExchange!"
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
		replica - object representing a given replica and it's associated
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
		replica - object representing a given replica and it's associated
		parameters
		"""
		input_name = self.inp_basename + "_" + \
					 str(replica.id) + "_" + \
					 str(replica.cycle) + ".md"
		output_name = self.inp_basename + "_" + \
					  str(replica.id) + "_" + \
					  str(replica.cycle) + ".out"

		k = Kernel(name="misc.ccount")
		k.arguments = ["--inputfile=" + \
					   input_name + " " + \
					   self.sh_file, "--outputfile=" + \
					   output_name]
		# no need to specify shared data here
		# everything in shared_files list will be staged in
		k.upload_input_data    = [input_name]
		k.download_output_data = output_name

		replica.cycle = replica.cycle + 1
		return k

	# --------------------------------------------------------------------------
	#
	def prepare_replica_for_exchange(self, replica):
		"""Launches matrix_calculator.py script on target resource in order to
		populate columns of swap matrix

		Arguments:
		replica - object representing a given replica and it's associated
		parameters
		"""

		matrix_col = "matrix_column_{cycle}_{replica}.dat"\
					 .format(cycle=replica.cycle-1, replica=replica.id )

		k = Kernel(name="md.re_exchange")
		k.arguments = ["--calculator=matrix_calculator.py",
					   "--replica_id=" + str(replica.id),
					   "--replica_cycle=" + str(replica.cycle-1),
					   "--replicas=" + str(self.replicas),
					   "--replica_basename=" + self.inp_basename]
		k.upload_input_data      = "matrix_calculator.py"
		k.download_output_data = matrix_col

		return k

	#---------------------------------------------------------------------------
	#
	def exchange(self, r_i, replicas, swap_matrix):
		"""Given replica r_i returns replica r_i needs to perform an exchange
		with

		Arguments:
		replicas - a list of replica objects
		swap_matrix - matrix of dimension-less energies, where each column is
		a replica and each row is a state
		"""
		return random.choice(replicas)

	#---------------------------------------------------------------------------
	#
	def get_swap_matrix(self, replicas, matrix_columns):
		"""Creates and populates swap matrix which is used to determine
		exchange probabilities

		Arguments:
		replicas - a list of replica objects
		matrix_columns - matrix of energy parameters obtained during the
		exchange step
		"""
		dim = len(replicas)

		# init matrix
		swap_matrix = [[ 0. for j in range(dim)] for i in range(dim)]

		matrix_columns = sorted(matrix_columns)

		# checking if matrix columns has enough rows
		if (len(matrix_columns) < dim):
			print "Ensemble MD Toolkit Error: matrix_columns does not have \
			enough rows."
			sys.exit()

		# checking if matrix columns rows have enough elements
		index = 0
		for row in matrix_columns:
			if (len(row) < dim):
				print "Ensemble MD Toolkit Error: matrix_columns row {0} does \
				not have enough elements.".format(index)
				sys.exit()
			index += 1

		for r in replicas:
			# populating one column at a time
			for i in range(len(replicas)):    
				pos = len(matrix_columns[r.id][i]) - 1
				if (matrix_columns[r.id][i][pos].isdigit()):
					swap_matrix[i][r.id] = float(matrix_columns[r.id][i])
				else:
					print "Ensemble MD Toolkit Error: matrix_columns element \
					({0},{1}) is not a number.".format(r.id, i)
					sys.exit()

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

	#---------------------------------------------------------------------------
	#
	def build_swap_matrix(self, replicas):
		"""Creates a swap matrix from matrix_column_x.dat files. 
		matrix_column_x.dat - is populated on targer resource and then 
		transferred back. This file is created for each replica and has data 
		for one column of swap matrix. In addition to that, this file holds 
		path to pilot compute unit of the previous run, where reside NAMD output
		files for a given replica. 

		Arguments:
		replicas - list of Replica objects

		Returns:
		swap_matrix - 2D list of lists of dimension-less energies, where each 
		column is a replica and each row is a state
		"""

		base_name = "matrix_column"
		size = len(replicas)

		# init matrix
		swap_matrix = [[ 0. for j in range(size)]
			 for i in range(size)]

		for r in replicas:
			column_file = base_name + "_" + \
						  str(r.cycle-1) + "_" + \
						  str(r.id) +  ".dat"       
			try:
				f = open(column_file)
				lines = f.readlines()
				f.close()
				data = lines[0].split()
				# populating one column at a time
				for i in range(size):
					swap_matrix[i][r.id] = float(data[i])
			except:
				raise

		return swap_matrix

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

		workdir_local = os.getcwd()

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
		cluster.run(re_pattern, force_plugin="replica_exchange.static_pattern_2")

		cluster.deallocate()
	
		#-----------------------------------------------------------------------

	except EnsemblemdError, er:

		print "Ensemble MD Toolkit Error: {0}".format(str(er))
		raise # Just raise the execption again to get the backtrace
