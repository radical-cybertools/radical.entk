#!/usr/bin/env python

""" 
This example shows how to use EnsembleMD Toolkit to execute RE scheme 2
with NAMD kernel

CURRENTLY THIS DOES NOT WORK!       

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python replica_exchange_2.py
"""

__author__       = "Antons Treikalis <antons.treikalis@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Replica Exchange using scheme 2 and NAMD kernel"


import os
import sys
import json
import math
import random
import optparse
from os import path

from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment
from radical.ensemblemd.patterns.ReplicaExchangePattern import Replica
from radical.ensemblemd.patterns.ReplicaExchangePattern import ReplicaExchangePattern

# ------------------------------------------------------------------------------
#

class ReScheme2(ReplicaExchangePattern):
    """
    """
    def __init__(self, inp_file):
        """Constructor.

        Arguments:
        inp_file - package input file with Pilot and NAMD related parameters as specified by user 
        """

        self.inp_basename = inp_file['input.MD']['basename']
        self.replicas = inp_file['input.MD']['nr_replicas']
        self.nr_cycles = inp_file['input.MD']['nr_cycles']

        super(ReScheme2, self).__init__(self.replicas, self.nr_cycles, self.inp_basename)

        # TODO

    # ------------------------------------------------------------------------------
    #
    def build_input_file(self, replica):
        """
        """

        basename = self.inp_basename[:-5]
        template = self.inp_basename
            
        new_input_file = "%s_%d_%d.namd" % (basename, replica.id, replica.cycle)
        outputname = "%s_%d_%d" % (basename, replica.id, replica.cycle)
        replica.new_coor = outputname + ".coor"
        replica.new_vel = outputname + ".vel"
        replica.new_history = outputname + ".history"
        replica.new_ext_system = outputname + ".xsc" 

        # TODO

    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_md(self, replica):
        """
        """
        # TODO

        self.build_input_file(replica)
        input_file = "%s_%d_%d.namd" % (self.inp_basename[:-5], replica.id, (replica.cycle-1))

        new_coor = replica.new_coor
        new_vel = replica.new_vel
        new_history = replica.new_history
        new_ext_system = replica.new_ext_system

        structure = self.work_dir_local + "/" + self.inp_folder + "/" + self.namd_structure
        coords = self.work_dir_local + "/" + self.inp_folder + "/" + self.namd_coordinates
        params = self.work_dir_local + "/" + self.inp_folder + "/" + self.namd_parameters

        # structure, coords and params must be transferred to resource as well, but these are
        # not passed as arguments to MD
        replica_md = Kernel(name="md.namd")
        replica_md.set_args("--inputfile=input_file")
        replica_md.set_download_output(files=[new_coor, new_vel, new_history, new_ext_system])

        return replica_md

    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_exchange(self, replica):
        """
        """
        # TODO       

        # name of the file which contains swap matrix column data for each replica
        matrix_col = "matrix_column_%s_%s.dat" % (replica.id, (replica.cycle-1))

        replica_ex = Kernel(name="misc.python")
        replica_ex.set_args(["--inputfile=[misc.re.calc, replica.id, (replica.cycle-1), nr_replicas, basename]", "--outputfile=matrix_col"])
        replica_ex.set_download_output(files=[matrix_col])

        return replica_ex

    #-------------------------------------------------------------------------------
    #
    def compose_swap_matrix(self, replicas):
        """
        """
        # init matrix
        swap_matrix = [[ 0. for j in range(len(replicas))] 
             for i in range(len(replicas))]
    
        #######
        # TODO
        #######

        return swap_matrix

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
            ps[r_j.id] = -(swap_matrix[r_i.sid][r_j.id] + swap_matrix[r_j.sid][r_i.id] - 
                      swap_matrix[r_i.sid][r_i.id] - swap_matrix[r_j.sid][r_j.id]) 

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

#-------------------------------------------------------------------------------
#
def parse_command_line():
    """Performs command line parsing.

    Returns:
    options - dictionary {'input_file': 'path/to/input.json'}
    """

    usage = "usage: %prog [Options]"
    parser = optparse.OptionParser(usage=usage)

    parser.add_option('--input',
              dest='input_file',
              help='specifies RadicalPilot, MD and RE simulation parameters')

    (options, args) = parser.parse_args()

    if options.input_file is None:
        parser.error("You must specify simulation input file (--input). Try --help for help.")

    return options

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
 
        # assuming that input file is passed as a command line argument
        work_dir_local = os.getcwd()
        params = parse_command_line()
    
        # get input file
        json_data=open(params.input_file)
        inp_file = json.load(json_data)
        json_data.close()

        # creating RE pattern object
        re_pattern = ReScheme2( inp_file )

        # initializing replica objects
        replicas = re_pattern.initialize_replicas()

        re_pattern.add_replicas( replicas )

        # run RE simulation  
        cluster.run(re_pattern, force_plugin="replica_exchange.static_pattern_2")

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
