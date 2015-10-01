#!/usr/bin/env python

"""
This example shows how to use the Ensemble MD Toolkit ``ReplicaExchange`` pattern.
Demonstrated RE simulation involves 16 replicas and performs a total of 3 synchronous simulation
cycles. Here exchange step is performed on target resource, which corresponds to ``static_pattern_2`` execution
plugin. Firstly, for each replica is generated dummy ``md_input_x_y.md`` input file and ``shared_md_input.dat``
shared file. Each of ``md_input_x_y.md`` files contains 500 randomly generated numbers, but ``shared_md_input.dat``
contains 500 characters which are both numbers and letters. As MD kernel in this example is used
``misc.ccount`` kernel which counts the number of occurrences of all characters in a given file.
As input file for this kernel is supplied previously generated ``md_input_x_y.md`` file. ``misc.ccount``
kernel produces ``md_input_x_y.out`` file, which is transferred to current working directory.
For exchange step is used ``md.re_exchange`` kernel which is supplied with ``matrix_calculator.py``
python script. This script is executed on target resource and simulates collection of output
parameters produced by MD step, which are required for exchange step. In this example ``matrix_calculator.py``
returns dummy parameter, which is a randomly generated number. This number does not affect the exchange
probability nor does it affect the choice of replica to perform an exchange with. Replica to perform an
exchange with is chosen randomly. Dummy replica parameter named ``parameter`` is exchanged during the
exchange step. Exchanges of ``parameter`` do not affect next simulation cycle.

.. figure:: ../images/replica_exchange_pattern.*
   :width: 300pt
   :align: center
   :alt: Replica Exchange Pattern

   Fig.: `Replica Exchange Pattern.`

Run Locally
^^^^^^^^^^^

.. warning:: In order to run this example, you need access to a MongoDB server 
             and set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** Download matrix calculator from :ref:`here <example_matrix_calculator>`.

**Step 2:** View and download the example :ref:`below <example_replica_exchange_b>`.

**Step 3:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to see log messages about simulation progress::

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
        project=None
    )

You can change the script to use a remote HPC cluster and increase the number of cores to see how this affects the runtime of the script as the individual
pipeline instances can run in parallel::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        project=None # add your allocation or project id here if required
    )

.. _example_matrix_calculator:

:download:`Download: matrix_calculator.py <../../examples/matrix_calculator.py>`

.. _example_replica_exchange_b:

Example Source
^^^^^^^^^^^^^^
"""

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

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        workdir_local = os.getcwd()

        t1 = datetime.datetime.utcnow()
        
        cluster = SingleClusterEnvironment(
            resource="localhost",
            cores=1,
            walltime=15,
            database_name='enmd-tests'
        )
        

        # Allocate the resources.
        cluster.allocate()

        # creating RE pattern object
        re_pattern = RePattern(workdir_local)

        t2 = datetime.datetime.utcnow()
        enmd_core_t1 = (t2-t1).total_seconds()

        t1 = datetime.datetime.utcnow()
        # set number of replicas
        re_pattern.replicas = 8
 
        # set number of cycles
        re_pattern.nr_cycles = 3

        # initializing replica objects
        replicas = re_pattern.initialize_replicas()

        re_pattern.add_replicas( replicas )

        t2 = datetime.datetime.utcnow()
        enmd_appl_t1 = (t2-t1).total_seconds()

        t3 = datetime.datetime.utcnow()

        # run RE simulation
        cluster.run(re_pattern, force_plugin="replica_exchange.static_pattern_2")

        t1 = datetime.datetime.utcnow()
        cluster.deallocate()
        t2 = datetime.datetime.utcnow()
        enmd_core_t2 = (t2-t1).total_seconds()
        
        print "RE simulation finished!"
        print "Simulation performed {0} cycles for {1} replicas. \
        In your working directory you should"\
               .format(re_pattern.nr_cycles, re_pattern.replicas)
        print "have {0} md_input_x_y.md files and {0} md_input_x_y.out \
        files where x in {{0,1,2,...{1}}} and y in {{0,1,...{2}}}."\
               .format( (re_pattern.nr_cycles*re_pattern.replicas), 
                        (re_pattern.replicas-1), 
                        (re_pattern.nr_cycles-1))
        print ".md file is replica input file and .out is output file \
        providing number of occurrences of each character."

        #-----------------------------------------------------------------------
        outfile = workdir_local + "/enmd-core-overhead.csv"
        try:
            f = open(outfile, 'a')
            row = "ENMD-core-timing-1: %f" % enmd_core_t1
            f.write("{row}\n".format(row=row))

            row = "ENMD-core-timing-2: %f" % enmd_core_t2
            f.write("{row}\n".format(row=row))

            row = "ENMD-appl-timing-1: %f" % enmd_appl_t1
            f.write("{row}\n".format(row=row))

            row = "ENMD-core-t-before cluster.run() call: {0}".format(t3)
            f.write("{row}\n".format(row=row))

            f.close()
        except IOError:
            print 'Warning: unable to access file %s' % outfile
            raise
        #-----------------------------------------------------------------------

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
