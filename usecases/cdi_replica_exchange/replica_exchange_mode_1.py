#!/usr/bin/env python
"""
This script is an example of simple application using the Ensemble MD Toolkit ``ReplicaExchange``
pattern for synchronous termerature-exchange RE simulation. Demonstrated RE simulation involves 32 
replicas and performs a total of 5 synchronous simulation cycles. Here exchange step is performed 
on target resource, which corresponds to ``static_pattern_2`` execution plugin. As MD application 
kernel in this use-case is used NAMD. Input files can be found  in ``/cdi_replica_exchange/namd_inp`` 
directory. Shared input files used by each replica are specified in prepare_shared_data() method. 
This method is called when specified files are to be transferred to the target resource. For this 
use-case these files are ``alanin.psf`` structure file, ``unfolded.pdb`` coordinates file and 
``alanin.params`` parameters file. After shared input files are transferred, for each replica is 
created input file using a template ``/cdi_replica_exchange/namd_inp/alanin_base.namd``. In this 
file are specified replica simulation parameters according to their initial values. After input 
files are created ``prepare_replica_for_md()`` is called and MD step is performed on a target 
resource. Then follows Exchange step. For each replica preparation required to perform this step 
on a target resource is specified in ``prepare_replica_for_exchange()``. After Exchange step is 
finished, final exchange procedure is performed locally according to Gibbs sampling method. This 
procedure is defined in ``get_swap_matrix()``, ``exchange()``, ``weighted_choice_sub()`` and 
``perform_swap()`` methods. After exchange procedure is finished the next MD run is performed and 
the process is then repeated. For remote exchange step is used ``/cdi_replica_exchange/namd_matrix_calculator.py`` 
python script. This script calculates one swap matrix column for replica by retrieving temperature 
and potential energy from simulation output file .history file.

Run Locally
^^^^^^^^^^^

.. warning:: In order to run this example, you need access to a MongoDB server and
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.
             In addition you need to have a local NAMD installation and NAMD should be
             invocable by calling ``namd2`` from terminal.


**Step 1:** View and download the example sources :ref:`below <cdi_replica_exchange>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python replica_exchange_mode_1.py

**Step 3:** Verify presence of generated input files alanin_base_x_y.namd where x is replica
            id and y is ccycle number.

Run Remotely
^^^^^^^^^^^^

By default, this use-case runs on your local machine::

    SingleClusterEnvironment(
        resource="localhost",
        cores=1,
        walltime=30,
        username=None,
        allocation=None
    )

You can change the script to use a remote HPC cluster::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        allocation=None # add your allocation or project id here if required
    )


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
    """Class ReplicaExchange must be extended by developer implementing RE application.
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
        self.replicas = 32
        self.work_dir_local = os.getcwd()
        self.nr_cycles = 5    
        self.namd_structure = "alanin.psf"
        self.namd_coordinates = "unfolded.pdb"
        self.namd_parameters = "alanin.params"

        # list holding paths to shared files 
        self.shared_urls = []
        # list holding names of shared files
        self.shared_files = []

        super(RePattern, self).__init__()

    # ------------------------------------------------------------------------------
    #
    def prepare_shared_data(self):
        """Populates shared_urls and shared_files lists.
        Files present in both of these lists will be transferred before simulation
        to the target resource.
        """
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
        """getter method for shared_urls list
        """
        return self.shared_urls

    #-------------------------------------------------------------------------------
    #
    def get_shared_files(self):
        """getter method for shared_files list
        """
        return self.shared_files

    # ------------------------------------------------------------------------------
    #
    def initialize_replicas(self):
        """Initializes replicas and their attributes

        Returns:
        replicas - a list of initialised ReplicaP objects
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
        replica - object representing a given replica and it's attributes

        Returns:
        k - an instance of Kernel class
        """

        self.build_input_file(replica)
        input_file = "%s_%d_%d.namd" % (self.inp_basename[:-5], replica.id, (replica.cycle))
        # this can be commented out
        output_file = replica.new_history

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
        # this can be commented out
        k.download_output_data = [str(output_file)]

        replica.cycle += 1
        return k
         
    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_exchange(self, replica):
        """Prepares md.re_exchange kernel to launch namd_matrix_calculator.py script 
        on target resource in order to populate columns of swap matrix.

        Arguments:
        replica - object representing a given replica and it's attributes
 
        Returns:
        k - an instance of Kernel class
        """

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

        Returns:
        swap_matrix - a matix of dimension-less energies
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

        print "RE simulation of %d cycles involving %d replicas has completed successfully!" % (re_pattern.nr_cycles, re_pattern.replicas)

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
