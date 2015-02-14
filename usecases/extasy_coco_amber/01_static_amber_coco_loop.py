#!/usr/bin/env python
"""
This script is an example to use the Ensemble MD Toolkit ``SimulationAnalysis``
pattern for the amber-coco usecase.


Run Locally
^^^^^^^^^^^^

This script cannot be run locally as it requires Amber and CoCo to be present in the target machine.


Run Remotely
^^^^^^^^^^^^

You can change the script to use a remote HPC cluster and increase the number
of cores to see how this affects the runtime of the script as the individual
simulation instances can run in parallel::
SingleClusterEnvironment(
resource="stampede.tacc.utexas.edu",            # label of the remote machine
cores=16,                                       # number of cores requested
walltime=30,                                    # walltime for the request
username=None,                                  # add your username here
allocation=None                                 # add your allocation or project id here if required
)

'numCUs' is the number of simulation instances per iteration.
'nsave' is the iteration at which backup needs to be created on the local machine.

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python 01_static_amber_coco_loop.py


"""

__author__        = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__     = "Copyright 2014, http://radical.rutgers.edu"
__license__       = "MIT"
__use_case_name__ = "'Amber + CoCo' simulation-analysis proof-of-concept (ExTASY)."


from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment
import imp
import argparse
import sys

# ------------------------------------------------------------------------------
#
class Extasy_CocoAmber_Static(SimulationAnalysisLoop):

    def __init__(self, maxiterations, simulation_instances, analysis_instances):
        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)

    def pre_loop(self):
        '''
        function : transfers input files, intermediate executables

        pre_coam_loop :-

                Purpose : Transfers files

                Arguments : None
        '''
        k = Kernel(name="md.pre_coam_loop")
        k.upload_input_data = [Kconfig.initial_crd_file,
                               Kconfig.md_input_file,
                               Kconfig.minimization_input_file,
                               Kconfig.top_file,
                               'postexec.py']
        return k


    def simulation_step(self, iteration, instance):
        '''
        function : if iteration = 1, use .crd file from pre_loop, else use .crd output from analysis generated
        in the previous iteration. Perform amber on the .crd files to generate a set of .ncdf files.

        amber :-

                Purpose : Run amber on each of the coordinate files. Currently, a non-MPI version of Amber is used.
                            Generates a .ncdf file in each instance.

                Arguments : --mininfile = minimization filename
                            --mdinfile  = MD input filename
                            --topfile   = Topology filename
                            --cycle     = current iteration number
        '''
        k = Kernel(name="md.amber")
        k.arguments = ["--mininfile={0}".format(Kconfig.minimization_input_file),
                       "--mdinfile={0}".format(Kconfig.md_input_file),
                       "--topfile={0}".format(Kconfig.top_file),
                       "--cycle=%s"%(iteration)]
        k.link_input_data = ['$PRE_LOOP/{0}'.format(Kconfig.minimization_input_file),
                             '$PRE_LOOP/{0}'.format(Kconfig.top_file),
                             '$PRE_LOOP/{0}'.format(Kconfig.md_input_file)]
        if((iteration-1)==0):
            k.link_input_data = k.link_input_data + ['$PRE_LOOP/{0} > min1.crd'.format(Kconfig.initial_crd_file)]
        else:
            k.link_input_data = k.link_input_data + ['$PREV_ANALYSIS_INSTANCE_1/min{0}{1}.crd > min{2}.crd'.format(iteration-1,instance-1,iteration)]
        if(iteration%Kconfig.nsave==0):
            k.download_output_data = ['md{0}.ncdf > backup/iter{0}/md_{0}_{1}.ncdf'.format(iteration,instance)]
        return k


    def analysis_step(self, iteration, instance):
        '''
        function : Perform CoCo Analysis on the output of the simulation from the current iteration. Using the .ncdf
         files generated in all the instance, generate the .crd file to be used in the next simulation.

        coco :-

                Purpose : Runs CoCo analysis on a set of .ncdf files and generates a coordinate file.

                Arguments : --grid          = Number of points along each dimension of the CoCo histogram
                            --dims          = The number of projections to consider from the input pcz file
                            --frontpoints   = Number of CUs
                            --topfile       = Topology filename
                            --mdfile        = MD Input filename
                            --output        = Output filename
                            --cycle         = Current iteration number
        '''
        k = Kernel(name="md.coco")
        k.arguments = ["--grid={0}".format(Kconfig.grid),
                       "--dims={0}".format(Kconfig.dims),
                       "--frontpoints={0}".format(Kconfig.num_CUs),
                       "--topfile={0}".format(Kconfig.top_file),
                       "--mdfile=*.ncdf",
                       "--output=pentaopt%s"%(iteration),
                       "--cycle=%s"%(iteration)]
        k.link_input_data = ['$PRE_LOOP/{0}'.format(Kconfig.top_file),
                             '$PRE_LOOP/postexec.py']
        k.cores = 16
        for iter in range(1,iteration+1):
            for i in range(1,num_sims+1):
                k.link_input_data = k.link_input_data + ['$SIMULATION_ITERATION_{0}_INSTANCE_{1}/md{0}.ncdf > md_{0}_{1}.ncdf'.format(iter,i)]
        return k

    def post_loop(self):
        pass


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.

        parser = argparse.ArgumentParser()
        parser.add_argument('--RPconfig', help='link to Radical Pilot related configurations file')
        parser.add_argument('--Kconfig', help='link to Kernel configurations file')

        args = parser.parse_args()

        if args.RPconfig is None:
            parser.error('Please enter a RP configuration file')
            sys.exit(1)
        if args.Kconfig is None:
            parser.error('Please enter a Kernel configuration file')
            sys.exit(0)

        RPconfig = imp.load_source('RPconfig', args.RPconfig)
        Kconfig = imp.load_source('Kconfig', args.Kconfig)


        cluster = SingleClusterEnvironment(
            resource=RPconfig.REMOTE_HOST,
            cores=RPconfig.PILOTSIZE,
            walltime=RPconfig.WALLTIME,
            username = RPconfig.UNAME, #username
            allocation = RPconfig.ALLOCATION, #allocation
	    queue = RPconfig.QUEUE
        )

        cluster.allocate()

        coco_amber_static = Extasy_CocoAmber_Static(maxiterations=Kconfig.num_iterations, simulation_instances=Kconfig.num_CUs, analysis_instances=1)
        cluster.run(coco_amber_static)

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
