#!/usr/bin/env python

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
import os

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
                               '{0}/postexec.py'.format(Kconfig.misc_loc)]
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
        k1 = Kernel(name="md.amber")
        k1.arguments = ["--mininfile={0}".format(os.path.basename(Kconfig.minimization_input_file)),
                       #"--mdinfile={0}".format(os.path.basename(Kconfig.md_input_file)),
                       "--topfile={0}".format(os.path.basename(Kconfig.top_file)),
                       "--crdfile={0}".format(os.path.basename(Kconfig.initial_crd_file)),
                       "--cycle=%s"%(iteration)]
        k1.link_input_data = ['$PRE_LOOP/{0}'.format(os.path.basename(Kconfig.minimization_input_file)),
                             '$PRE_LOOP/{0}'.format(os.path.basename(Kconfig.top_file)),
                             '$PRE_LOOP/{0}'.format(os.path.basename(Kconfig.initial_crd_file))]
        k1.cores=1
        if((iteration-1)==0):
            k1.link_input_data = k1.link_input_data + ['$PRE_LOOP/{0} > min1.crd'.format(os.path.basename(Kconfig.initial_crd_file))]
        else:
            k1.link_input_data = k1.link_input_data + ['$PREV_ANALYSIS_INSTANCE_1/min{0}{1}.crd > min{2}.crd'.format(iteration-1,instance-1,iteration)]
        k1.copy_output_data = ['md{0}.crd > $PRE_LOOP/md_{0}_{1}.crd'.format(iteration,instance)]
        

        k2 = Kernel(name="md.amber")
        k2.arguments = [
                            "--mdinfile={0}".format(os.path.basename(Kconfig.md_input_file)),
                            "--topfile={0}".format(os.path.basename(Kconfig.top_file)),
                            "--cycle=%s"%(iteration)
			    
                        ]
        k2.link_input_data = [  
                                "$PRE_LOOP/{0}".format(os.path.basename(Kconfig.md_input_file)),
                                "$PRE_LOOP/{0}".format(os.path.basename(Kconfig.top_file)),
                                "$PRE_LOOP/md_{0}_{1}.crd > md{0}.crd".format(iteration,instance),
                            ]
        if(iteration%Kconfig.nsave==0):
            k2.download_output_data = ['md{0}.ncdf > output/iter{0}/md_{0}_{1}.ncdf'.format(iteration,instance)]

        k2.cores = 1
        return [k1,k2]


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
        k1 = Kernel(name="md.coco")
        k1.arguments = ["--grid={0}".format(Kconfig.grid),
                       "--dims={0}".format(Kconfig.dims),
                       "--frontpoints={0}".format(Kconfig.num_CUs),
                       "--topfile={0}".format(os.path.basename(Kconfig.top_file)),
                       "--mdfile=*.ncdf",
                       "--output=pdbs",
                       "--atom_selection={0}".format(Kconfig.atom_selection)]
        k1.cores = min(Kconfig.num_CUs,RPconfig.PILOTSIZE)
        k1.uses_mpi = True

        k1.link_input_data = ['$PRE_LOOP/{0}'.format(os.path.basename(Kconfig.top_file))]
        for iter in range(1,iteration+1):
            for i in range(1,Kconfig.num_CUs+1):
                k1.link_input_data = k1.link_input_data + ['$SIMULATION_ITERATION_{0}_INSTANCE_{1}/md{0}.ncdf > md_{0}_{1}.ncdf'.format(iter,i)]

        k1.copy_output_data = list()
        for i in range(0,Kconfig.num_CUs):
            k1.copy_output_data = k1.copy_output_data + ['pdbs{1}.pdb > $PRE_LOOP/pentaopt{0}{1}.pdb'.format(iteration,i)]

        if(iteration%Kconfig.nsave==0):
            k1.download_output_data = ['coco.log > output/iter{0}/coco.log'.format(iteration,instance)]


        k2 = Kernel(name="md.tleap")
        k2.arguments = ["--numofsims={0}".format(Kconfig.num_CUs),
                        "--cycle={0}".format(iteration)]

        k2.link_input_data = ['$PRE_LOOP/postexec.py > postexec.py']
        for i in range(0,Kconfig.num_CUs):
            k2.link_input_data = k2.link_input_data + ['$PRE_LOOP/pentaopt{0}{1}.pdb > pentaopt{0}{1}.pdb'.format(iteration,i)]

        return [k1,k2]

    def post_loop(self):
        pass


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:


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

        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.

        cluster = SingleClusterEnvironment(
            resource=RPconfig.REMOTE_HOST,
            cores=RPconfig.PILOTSIZE,
            walltime=RPconfig.WALLTIME,
            username = RPconfig.UNAME, #username
            project = RPconfig.ALLOCATION, #project
          queue = RPconfig.QUEUE,
            database_url = RPconfig.DBURL
        )

        cluster.allocate()

        coco_amber_static = Extasy_CocoAmber_Static(maxiterations=Kconfig.num_iterations, simulation_instances=Kconfig.num_CUs, analysis_instances=1)
        cluster.run(coco_amber_static)

        cluster.deallocate()

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
