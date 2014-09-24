#!/usr/bin/env python

""" 
This example shows how to use EnsembleMD Toolkit to execute sycnhronous RE pattern with NAMD kernel

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::
RADICAL_ENMD_VERBOSE=info python replica_exchange_2.py
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
    def __init__(self, my_id, new_temperature=None, cores=1):
        """Constructor.

        Arguments:
        my_id - integer representing replica's id
        """
        self.id = int(my_id)
        self.cycle = 0
        
        super(ReplicaP, self).__init__(my_id)

class RePattern(ReplicaExchange):
    """
    """
    def __init__(self):
        """Constructor.
        """

        self.inp_basename = "simula.md"
        self.replicas = 4
        self.work_dir_local = os.getcwd()
        self.nr_cycles = 3     
        self.structure = "protein.abcd"

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
        """

    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_md(self, replica):
        """
        """

        k = Kernel(name="misc.toy_md")
        k.set_args(replica.id, (replica.cycle-1), self.replicas, basename)
        k.download_output_data(matrix_col)

        return k
         
    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_exchange(self, replica):
        """
        For this example not needed...
        """
        pass

    #-------------------------------------------------------------------------------
    #
    def exchange(self, r_i, replicas, swap_matrix):
        """
        """
        return random.choice(replicas)

    #-------------------------------------------------------------------------------
    #
    def get_swap_matrix(self, replicas):
        """
        """
        pass


    #-------------------------------------------------------------------------------
    #
    def perform_swap(self, replica_i, replica_j):
        """
        """
        pass

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