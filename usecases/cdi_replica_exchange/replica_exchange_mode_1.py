#!/usr/bin/env python

__author__        = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__     = "Copyright 2014, http://radical.rutgers.edu"
__license__       = "MIT"
__use_case_name__ = "CDI Replica Exchange: 'mode 1'."


import os
import sys
import json
import math
import random
import pprint
import optparse
from os import path

from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment
from radical.ensemblemd.patterns.replica_exchange import Replica
from radical.ensemblemd.patterns.replica_exchange import ReplicaExchange

#-------------------------------------------------------------------------------
#

class ReplicaP(Replica):
	"""Class representing replica and it's parameters.
	Class Replica must be extended by developer writing RE application.
	"""
	def __init__(self, my_id, new_temperature=None, cores=1):
		"""Constructor.

		Arguments:
		my_id - integer representing replica's id
		new_temperature - initial temperature assigned to this replica 
		cores - number of cores each replica should use
		"""
		self.id = int(my_id)
		self.sid = int(my_id)
		self.cycle = 0
		if new_temperature is None:
			self.new_temperature = 0
		else:
			self.new_temperature = new_temperature
		self.old_temperature = new_temperature
		self.potential = 0

		self.new_coor = ""
		self.new_vel = ""
		self.new_history = ""
		self.new_ext_system = "" 
		self.old_coor = ""
		self.old_vel = ""
		self.old_ext_system = "" 
		self.old_path = ""
		self.first_path = ""
		self.swap = 0
		self.cores = cores

		# calling constructor of parent class
		super(ReplicaP, self).__init__(my_id)

class RePattern(ReplicaExchange):
	"""Class ReplicaExchange must be extended by developer implementing RE 
	application.
	In this class are specified details of RE simulation:
		- initialization of replicas
		- generation of input files
		- preparation for MD and Exchange steps
		- implementation of exchange routines
	"""

	def __init__(self):
		"""Constructor.
		In principle simulation parameters should be passed using input file.
		"""

		# currently all parameters are hardcoded
		self.inp_basename = "alanin_base.namd"
		self.inp_folder = "namd_inp"
		self.min_temp = 300.0
		self.max_temp = 600.0
		self.cycle_steps = 1000
		self.work_dir_local = os.getcwd() 
		self.namd_structure = "alanin.psf"
		self.namd_coordinates = "unfolded.pdb"
		self.namd_parameters = "alanin.params"
		self.replicas = None
		self.nr_cycles = None

		# list holding paths to shared files 
		self.shared_urls = []
		# list holding names of shared files
		self.shared_files = []

		super(RePattern, self).__init__()

	# --------------------------------------------------------------------------
	#
	def prepare_shared_data(self):
		"""Populates shared_urls and shared_files lists.
		Files present in both of these lists will be transferred before 
		simulation to the target resource.
		"""
		structure_path = self.work_dir_local + "/" + self.inp_folder + "/" + \
						 self.namd_structure
		coords_path = self.work_dir_local + "/" + self.inp_folder + "/" + \
					  self.namd_coordinates
		params_path = self.work_dir_local + "/" + self.inp_folder + "/" + \
					  self.namd_parameters

		self.shared_files.append(self.namd_structure)
		self.shared_files.append(self.namd_coordinates)
		self.shared_files.append(self.namd_parameters)

		struct_url = 'file://%s' % (structure_path)
		self.shared_urls.append(struct_url)
 
		coords_url = 'file://%s' % (coords_path)
		self.shared_urls.append(coords_url)     

		params_url = 'file://%s' % (params_path)
		self.shared_urls.append(params_url)

	# --------------------------------------------------------------------------
	#
	def initialize_replicas(self):
		"""Initializes replicas and their attributes

		Returns:
		replicas - a list of initialised ReplicaP objects
		"""
		try:
			self.replicas+1
		except:        
			 print "Ensemble MD Toolkit Error: Number of replicas must be \
			 defined for pattern ReplicaExchnage"
			 raise

		replicas = []
		N = self.replicas
		factor = (self.max_temp/self.min_temp)**(1./(N-1))
		for k in range(N):
			new_temp = self.min_temp * (factor**k)
			r = ReplicaP(k, new_temp)
			replicas.append(r)
			
		return replicas

	# --------------------------------------------------------------------------
	#
	def build_input_file(self, replica):
		"""Generates NAMD input file based on template.

		Arguments:
		replica - object representing a given replica and it's attributes
		(an instance of ReplicaP class)
		"""

		basename = self.inp_basename[:-5]
		template = self.inp_basename
			
		new_input_file = "%s_%d_%d.namd" % (basename, replica.id, replica.cycle)
		outputname = "%s_%d_%d" % (basename, replica.id, replica.cycle)
		old_name = "%s_%d_%d" % (basename, replica.id, (replica.cycle-1))
		replica.new_coor = outputname + ".coor"
		replica.new_vel = outputname + ".vel"
		replica.new_history = outputname + ".history"
		replica.new_ext_system = outputname + ".xsc" 
		historyname = replica.new_history

		if (replica.cycle == 0):
			first_step = 0
		elif (replica.cycle == 1):
			first_step = int(self.cycle_steps)
		else:
			first_step = (replica.cycle - 1) * int(self.cycle_steps)

		if (replica.cycle == 0):
			old_name = "%s_%d_%d" % (basename, replica.id, (replica.cycle-1)) 
			structure = self.namd_structure
			coordinates = self.namd_coordinates
			parameters = self.namd_parameters
		else:
			old_name = "../staging_area/%s_%d_%d" % (basename, \
													 replica.id, \
													(replica.cycle-1))
			structure = self.namd_structure
			coordinates = self.namd_coordinates
			parameters = self.namd_parameters

		# substituting tokens in main replica input file 
		try:
			r_file = open( (os.path.join((self.work_dir_local + "/namd_inp/"), \
							template)), "r")
		except IOError:
			print 'Warning: unable to access template file %s' % template

		tbuffer = r_file.read()
		r_file.close()

		tbuffer = tbuffer.replace("@swap@",str(replica.swap))
		tbuffer = tbuffer.replace("@ot@",str(replica.old_temperature))
		tbuffer = tbuffer.replace("@nt@",str(replica.new_temperature))
		tbuffer = tbuffer.replace("@steps@",str(self.cycle_steps))
		tbuffer = tbuffer.replace("@rid@",str(replica.id))
		tbuffer = tbuffer.replace("@somename@",str(outputname))
		tbuffer = tbuffer.replace("@oldname@",str(old_name))
		tbuffer = tbuffer.replace("@cycle@",str(replica.cycle))
		tbuffer = tbuffer.replace("@firststep@",str(first_step))
		tbuffer = tbuffer.replace("@history@",str(historyname))
		tbuffer = tbuffer.replace("@structure@", str(structure))
		tbuffer = tbuffer.replace("@coordinates@", str(coordinates))
		tbuffer = tbuffer.replace("@parameters@", str(parameters))
		
		# write out
		try:
			w_file = open( new_input_file, "w")
			w_file.write(tbuffer)
			w_file.close()
		except IOError:
			print 'Warning: unable to access file %s' % new_input_file

	# --------------------------------------------------------------------------
	#
	def prepare_replica_for_md(self, replica):
		"""Specifies input and output files and passes them to NAMD kernel

		Arguments:
		replica - object representing a given replica and it's attributes

		Returns:
		k - an instance of Kernel class
		"""

		self.build_input_file(replica)
		input_file = "%s_%d_%d.namd" % (self.inp_basename[:-5], \
										replica.id, \
										(replica.cycle))
		# this can be commented out
		output_file = replica.new_history

		new_coor = replica.new_coor
		new_vel = replica.new_vel
		new_history = replica.new_history
		new_ext_system = replica.new_ext_system

		old_coor = replica.old_coor
		old_vel = replica.old_vel
		old_ext_system = replica.old_ext_system 

		copy_out = []
		copy_out.append(new_history)
		copy_out.append(new_coor)
		copy_out.append(new_vel)
		copy_out.append(new_ext_system)

		k = Kernel(name="md.namd")
		k.arguments            = [input_file]
		k.upload_input_data    = [str(input_file)] 
		k.copy_output_data = copy_out
		k.download_output_data = new_history

		replica.cycle += 1
		return k
		 
	# --------------------------------------------------------------------------
	#
	def prepare_replica_for_exchange(self, replica):
		"""Prepares md.re_exchange kernel to launch namd_matrix_calculator.py 
		script on target resource in order to populate columns of swap matrix.

		Arguments:
		replica - object representing a given replica and it's attributes
 
		Returns:
		k - an instance of Kernel class
		"""

		basename = self.inp_basename[:-5]

		matrix_col = "matrix_column_{cycle}_{replica}.dat"\
					 .format(cycle=replica.cycle-1, replica=replica.id )

		k = Kernel(name="md.re_exchange")
		k.arguments = ["--calculator=namd_matrix_calculator.py", 
					   "--replica_id=" + str(replica.id), 
					   "--replica_cycle=" + str(replica.cycle-1), 
					   "--replicas=" + str(self.replicas), 
					   "--replica_basename=" + str(basename)]

		k.upload_input_data    = "namd_matrix_calculator.py"
		k.download_output_data = matrix_col

		return k

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

	#---------------------------------------------------------------------------
	#
	def perform_swap(self, replica_i, replica_j):
		"""Performs an exchange of temperatures

		Arguments:
		replica_i - a replica object
		replica_j - a replica object
		"""

		# swap temperatures
		temperature = replica_j.new_temperature
		replica_j.new_temperature = replica_i.new_temperature
		replica_i.new_temperature = temperature
		# record that swap was performed
		replica_i.swap = 1
		replica_j.swap = 1

	#---------------------------------------------------------------------------
	#
	def exchange(self, r_i, replicas, swap_matrix):
		"""Adopted from asyncre-bigjob [1]
		Produces a replica "j" to exchange with the given replica "i"
		based off independence sampling of the discrete distribution

		Arguments:
		r_i - given replica for which is found partner replica
		replicas - list of Replica objects
		swap_matrix - matrix of dimension-less energies, where each column is a 
		replica and each row is a state

		Returns:
		r_j - replica to exchnage parameters with
		"""
		#evaluate all i-j swap probabilities
		ps = [0.0]*(self.replicas)
  
		for r_j in replicas:
			ps[r_j.id] = -(swap_matrix[r_i.sid][r_j.id] + \
						   swap_matrix[r_j.sid][r_i.id] - \
						   swap_matrix[r_i.sid][r_i.id] - \
						   swap_matrix[r_j.sid][r_j.id]) 

		new_ps = []
		for item in ps:
			new_item = math.exp(item)
			new_ps.append(new_item)
		ps = new_ps
		# index of swap replica within replicas_waiting list
		j = len(replicas)
		while j > (len(replicas) - 1):
			j = self.weighted_choice_sub(ps)
		
		# actual replica
		r_j = replicas[j]
		return r_j

	#---------------------------------------------------------------------------
	#
	def weighted_choice_sub(self, weights):
		"""Adopted from asyncre-bigjob [1]
		"""

		rnd = random.random() * sum(weights)
		for i, w in enumerate(weights):
			rnd -= w
			if rnd < 0:
				return i    

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

	try:
		# Create a new static execution context with one resource and a fixed
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

		# creating RE pattern object
		re_pattern = RePattern()
   
		# set number of replicas
		re_pattern.replicas = 32
 
		# set number of cycles
		re_pattern.nr_cycles = 3

		# initializing replica objects
		replicas = re_pattern.initialize_replicas()

		re_pattern.add_replicas( replicas )

		# run RE simulation  
		cluster.run(re_pattern, force_plugin="replica_exchange.static_pattern_2")

		cluster.deallocate()

		#print "Simulation finished!"
		#print "Simulation performed {0} cycles with {1} replicas. In your working directory you should".format(re_pattern.nr_cycles, re_pattern.replicas)
		#print "have {0} alanin_base_x_y.namd files and {0} alanin_base_x_y.history files where x in {{0,1,2,...{1}}} and y in {{0,1,...{2}}}.".format( (re_pattern.nr_cycles*re_pattern.replicas), (re_pattern.replicas-1), (re_pattern.nr_cycles-1))

	except EnsemblemdError, er:

		print "Ensemble MD Toolkit Error: {0}".format(str(er))
		raise # Just raise the execption again to get the backtrace
