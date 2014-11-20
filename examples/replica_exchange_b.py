#!/usr/bin/env python

""" 
Run Locally 
^^^^^^^^^^^

.. warning:: In order to run this example, you need access to a MongoDB server and 
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly. 
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** View and download the example sources :ref:`below <example_replica_exchange_b>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python replica_exchange_b.py

TODO Antons: describe how to check the output.

Run Remotely
^^^^^^^^^^^^

By default, the exchange steps and the analysis run on one core your local machine:: 

    SingleClusterEnvironment(
        resource="localhost", 
        cores=1, 
        walltime=30,
        username=None,
        allocation=None
    )

You can change the script to use a remote HPC cluster and increase the number 
of cores to see how this affects the runtime of the script as the individual
pipeline instances can run in parallel::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu", 
        cores=16, 
        walltime=30,
        username=None,  # add your username here 
        allocation=None # add your allocation or project id here if required
    )

.. _example_replica_exchange_b:

Example Source 
^^^^^^^^^^^^^^
"""
 
__author__       = "Antons Treikalis <antons.treikalis@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Synchronous Replica Exchange with 'remote' exchange (generic)."


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
        self.parameter = random.randint(300, 600)
        self.cycle = 0
        
        super(ReplicaP, self).__init__(my_id)

class RePattern(ReplicaExchange):
    """
    """
    def __init__(self):
        """Constructor
        """
        # hardcoded name of the input file base
        self.inp_basename = "simula"
        # number of replicas to be launched during the simulation
        self.replicas = 4
        # number of cycles the simulaiton will perform
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
        Generates dummy input file
        """

        file_name = self.inp_basename + "_" + str(replica.id) + "_" + str(replica.cycle) + ".md"

        fo = open(file_name, "wb")
        for i in range(1,500):
            fo.write(str(random.randint(i, 500) + i*2.5) + " ");
            if i % 10 == 0:
                fo.write(str("\n"));
        fo.close()

    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_md(self, replica):
        """
        Specifies input and output files and passes them to kernel
        """
        input_name = self.inp_basename + "_" + str(replica.id) + "_" + str(replica.cycle) + ".md"
        output_name = self.inp_basename + "_" + str(replica.id) + "_" + str(replica.cycle) + ".out"

        k = Kernel(name="misc.ccount")
        k.arguments            = ["--inputfile=" + input_name, "--outputfile=" + output_name]
        k.upload_input_data      = input_name
        k.download_output_data = output_name

        replica.cycle = replica.cycle + 1
        return k
         
    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_exchange(self, replica):
        """
        launches matrix_calculator.py script on target resource in order to populate columns of swap matrix
        """
        output_name = "matrix_column_%s_%s.dat" % ( replica.id, (replica.cycle-1) ) 

        k = Kernel(name="md.re_exchange")
        k.arguments = ["--calculator=matrix_calculator.py", 
                       "--replica_id=" + str(replica.id), 
                       "--replica_cycle=" + str(replica.cycle-1), 
                       "--replicas=" + str(self.replicas), 
                       "--replica_basename=" + self.inp_basename]
        k.upload_input_data      = "matrix_calculator.py"
        k.download_output_data = output_name

        return k

    #-------------------------------------------------------------------------------
    #
    def exchange(self, r_i, replicas, swap_matrix):
        """
        Given replica r_i returns replica r_i needs to perform an exchange with
        """
        return random.choice(replicas)

    #-------------------------------------------------------------------------------
    #
    def get_swap_matrix(self, replicas):
        """
        Creates and populates swap matrix which is used to determine exchange probabilities
        """
        # init matrix
        swap_matrix = [[ 0. for j in range(len(replicas))] 
             for i in range(len(replicas))]

        for r in replicas:
            column_file = "matrix_column" + "_" + str(r.id) + "_" + str(r.cycle-1) + ".dat"       
            try:
                f = open(column_file)
                lines = f.readlines()
                f.close()
                data = lines[0].split()
                # populating one column at a time
                for i in range(len(replicas)):
                    swap_matrix[i][r.id] = float(data[i])
            except:
                raise

        return swap_matrix

    #-------------------------------------------------------------------------------
    #
    def perform_swap(self, replica_i, replica_j):
        """
        Performs an exchange of desired parameters
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
        cluster.run(re_pattern, force_plugin="replica_exchange.static_pattern_2")
        print "RE simulation finished!"
        print "Simulation performed 3 cycles for 4 replicas. In your working directory you should"
        print "have 12 simula_x_y.md files and 12 simula_x_y.out files ( x in {0,1,2,3}; y in {0,1,2} ) "
        print "where .md file is replica input file and .out is output providing number of occurrences"
        print "of each character. \n"

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
