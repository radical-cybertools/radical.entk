#!/usr/bin/env python

""" 
This example shows how to use EnsembleMD Toolkit to execute sycnhronous RE pattern with NAMD kernel

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::
RADICAL_ENMD_VERBOSE=info python replica_exchange.py
"""
 
__author__       = "Antons Treikalis <antons.treikalis@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Replica Exchange using synchronous pattern and NAMD kernel"


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
    def __init__(self, my_id, cores=1):
        """Constructor.

        Arguments:
        my_id - integer representing replica's id
        """
        self.id = int(my_id)
        self.parameter = int(my_id) * 2
        self.cycle = 0
        
        super(ReplicaP, self).__init__(my_id)

class RePattern(ReplicaExchange):
    """
    """
    def __init__(self):
        """Constructor.
        """
        self.inp_basename = "simula"
        self.replicas = 4
        self.work_dir_local = os.getcwd()
        self.nr_cycles = 3     

        super(RePattern, self).__init__()

    # ------------------------------------------------------------------------------
    #
    def initialize_replicas(self):
        """Initializes replicas and their attributes to default values
        """
        replicas = []
        N = self.replicas
        for k in range(N):
            r = ReplicaP(k)
            replicas.append(r)
            
        return replicas

    # ------------------------------------------------------------------------------
    #
    def build_input_file(self, replica):
        """
        ok
        """
        file_name = self.inp_basename + "_" + replica.id + ".md"
        fo = open(file_name, "wb")
        fo.write( "100, 120, 140, 160, 180, 200, 220, 240, 260, 280");
        fo.close()

    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_md(self, replica):
        """
        ok
        """
        input_name = self.inp_basename + "_" + str(replica.id) + ".md"
        output_name = self.inp_basename + "_" + str(replica.id) + ".out"

        k = Kernel(name="misc.ccount")
        k.arguments            = ["--inputfile=" + input_name, "--outputfile=" + output_name]
        k.link_input_data      = input_name
        k.download_output_data = output_name
        return k
         
    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_exchange(self, replica):
        """
        ok
        """
        pass

    #-------------------------------------------------------------------------------
    #
    def exchange(self, r_i, replicas, swap_matrix):
        """
        ok
        """
        return random.choice(replicas)

    #-------------------------------------------------------------------------------
    #
    def get_swap_matrix(self, replicas):
        """
        ok
        """
        # init matrix
        swap_matrix = [[ 0. for j in range(len(replicas))] 
            for i in range(len(replicas))]

        return swap_matrix

    #-------------------------------------------------------------------------------
    #
    def perform_swap(self, replica_i, replica_j):
        """
        ok
        """
        param_i = replica_i.parameter
        replica_i.parameter = replica_j.parameter
        replica_j.parameter = param_i

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

        # creating RE pattern object
        re_pattern = RePattern()

        # initializing replica objects
        replicas = re_pattern.initialize_replicas()

        re_pattern.add_replicas( replicas )

        # run RE simulation  
        cluster.run(re_pattern, force_plugin="replica_exchange.static_pattern_1")

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace