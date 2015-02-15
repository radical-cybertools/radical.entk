#!/usr/bin/env python

""" 
CDI Replica Exchange: 'mode 1'.
This example demonstrates how application for synchronous termerature-exchange RE 
simulation with NAMD kernel can be implemented. 

To run this example from terminal use:
RADICAL_ENMD_VERBOSE=info python replica_exchange_mode_1.py
"""

__author__        = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__     = "Copyright 2014, http://radical.rutgers.edu"
__license__       = "MIT"
__use_case_name__ = "CDI Replica Exchange: 'mode 1'."


from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

import os
import sys
import json
import math
import random
import optparse
from os import path
import radical.pilot

from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment
from radical.ensemblemd.patterns.replica_exchange import Replica
from radical.ensemblemd.patterns.replica_exchange import ReplicaExchange

#------------------------------------------------------------------------------
#

class ReplicaP(Replica):
    """Class representing replica and it's associated data.

    This will have to be extended by users implementing RE pattern for 
    a particular kernel and scheme
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

        super(ReplicaP, self).__init__(my_id)

class RePattern(ReplicaExchange):
    """In this class are specified details of RE simulation:
        - initialization of replicas
        - generation of input files
        - preparation for MD and exchange steps
        - implementation of exchange routines
    """

    def __init__(self):
        """Constructor.
        """

        # currently all params are hardcoded
        self.inp_basename = "alanin_base.namd"
        self.inp_folder = "namd_inp"
        self.min_temp = 300.0
        self.max_temp = 600.0
        self.cycle_steps = 1000
        self.replicas = 32
        self.work_dir_local = os.getcwd()
        self.nr_cycles = 3    
        self.namd_structure = "alanin.psf"
        self.namd_coordinates = "unfolded.pdb"
        self.namd_parameters = "alanin.params"

        self.shared_urls = []
        self.shared_files = []

        super(RePattern, self).__init__()

    # ------------------------------------------------------------------------------
    #
    def prepare_shared_data(self):
 
        structure_path = self.work_dir_local + "/" + self.inp_folder + "/" + self.namd_structure
        coords_path = self.work_dir_local + "/" + self.inp_folder + "/" + self.namd_coordinates
        params_path = self.work_dir_local + "/" + self.inp_folder + "/" + self.namd_parameters

        self.shared_files.append(self.namd_structure)
        self.shared_files.append(self.namd_coordinates)
        self.shared_files.append(self.namd_parameters)

        struct_url = 'file://%s' % (structure_path)
        self.shared_urls.append(struct_url)
 
        coords_url = 'file://%s' % (coords_path)
        self.shared_urls.append(coords_url)     

        params_url = 'file://%s' % (params_path)
        self.shared_urls.append(params_url)

    #-------------------------------------------------------------------------------
    #
    def get_shared_urls(self):
        return self.shared_urls

    #-------------------------------------------------------------------------------
    #
    def get_shared_files(self):
        return self.shared_files

    # ------------------------------------------------------------------------------
    #
    def initialize_replicas(self):
        """Initializes replicas and their attributes to default values
        """
        replicas = []
        N = self.replicas
        factor = (self.max_temp/self.min_temp)**(1./(N-1))
        for k in range(N):
            new_temp = self.min_temp * (factor**k)
            r = ReplicaP(k, new_temp)
            replicas.append(r)
            
        return replicas

    # ------------------------------------------------------------------------------
    #
    def build_input_file(self, replica):
        """Generates input file for NAMD

        Arguments:
        replica - object representing a given replica and it's associated parameters
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
            old_name = replica.old_path + "/%s_%d_%d" % (basename, replica.id, (replica.cycle-1))
            structure = self.namd_structure
            coordinates = self.namd_coordinates
            parameters = self.namd_parameters

        # substituting tokens in main replica input file 
        try:
            r_file = open( (os.path.join((self.work_dir_local + "/namd_inp/"), template)), "r")
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

    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_md(self, replica):
        """Specifies input and output files and passes them to NAMD kernel

        Arguments:
        replica - object representing a given replica and it's associated parameters
        """

        self.build_input_file(replica)
        input_file = "%s_%d_%d.namd" % (self.inp_basename[:-5], replica.id, (replica.cycle))

        new_coor = replica.new_coor
        new_vel = replica.new_vel
        new_history = replica.new_history
        new_ext_system = replica.new_ext_system

        old_coor = replica.old_coor
        old_vel = replica.old_vel
        old_ext_system = replica.old_ext_system 

        k = Kernel(name="md.namd")
        k.arguments            = [input_file]
        k.upload_input_data    = [str(input_file)]

        replica.cycle += 1
        return k
         
    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_exchange(self, replica):
        """Prepares md.re_exchange kernel to launch namd_matrix_calculator.py script on target resource 
        in order to populate columns of swap matrix.

        Arguments:
        replica - object representing a given replica and it's associated parameters
        """
        # name of the file which contains swap matrix column data for each replica
        matrix_col = "matrix_column_%s_%s.dat" % (replica.id, (replica.cycle-1))
        basename = self.inp_basename[:-5]

        k = Kernel(name="md.re_exchange")
        k.arguments = ["--calculator=namd_matrix_calculator.py", 
                       "--replica_id=" + str(replica.id), 
                       "--replica_cycle=" + str(replica.cycle-1), 
                       "--replicas=" + str(self.replicas), 
                       "--replica_basename=" + str(basename)]
        k.upload_input_data      = "namd_matrix_calculator.py"

        return k

    #-------------------------------------------------------------------------------
    #
    def get_swap_matrix(self, replicas, matrix_columns):
        """Creates and populates swap matrix which is used to determine exchange probabilities

        Arguments:
        replicas - a list of replica objects
        matrix_columns - matrix of energy parameters obtained during the exchange step
        """

        base_name = "matrix_column"
 
        # init matrix
        swap_matrix = [[ 0. for j in range(len(replicas))] 
            for i in range(len(replicas))]

        for r in replicas:
            # populating one column at a time
            for i in range(len(replicas)):
                swap_matrix[i][r.id] = float(matrix_columns[r.id][i])

            # setting old_path and first_path for each replica
            if ( r.cycle == 1 ):
                r.first_path = matrix_columns[r.id][len(replicas)]
                r.old_path = matrix_columns[r.id][len(replicas)]
            else:
                r.old_path = matrix_columns[r.id][len(replicas)]

        return swap_matrix

    #-------------------------------------------------------------------------------
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

    #-------------------------------------------------------------------------------
    #
    def exchange(self, r_i, replicas, swap_matrix):
        """Adopted from asyncre-bigjob [1]
        Produces a replica "j" to exchange with the given replica "i"
        based off independence sampling of the discrete distribution

        Arguments:
        r_i - given replica for which is found partner replica
        replicas - list of Replica objects
        swap_matrix - matrix of dimension-less energies, where each column is a replica 
        and each row is a state

        Returns:
        r_j - replica to exchnage parameters with
        """
        #evaluate all i-j swap probabilities
        ps = [0.0]*(self.replicas)
  
        for r_j in replicas:
            ps[r_j.id] = -(swap_matrix[r_i.sid][r_j.id] + swap_matrix[r_j.sid][r_i.id] - swap_matrix[r_i.sid][r_i.id] - swap_matrix[r_j.sid][r_j.id]) 

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

    #------------------------------------------------------------------------------
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
            walltime=15
        )
        

        # Allocate the resources.
        cluster.allocate()

        # creating RE pattern object
        re_pattern = RePattern()

        # initializing replica objects
        replicas = re_pattern.initialize_replicas()

        re_pattern.add_replicas( replicas )

        # run RE simulation  
        cluster.run(re_pattern, force_plugin="replica_exchange.static_pattern_2")

        print "RE simulation of 3 cycles involving 32 replicas has completed successfully!"

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
