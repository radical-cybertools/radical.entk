#!/usr/bin/env python

"""

This example shows how to use the Ensemble MD Toolkit ``SimulationAnalysis``
pattern for the Amber-CoCo usecase which has multiple Amber based Simulation
instances and a single CoCo Analysis stage. Although the user is free to use
any method to mention the inputs, this usecase example uses two configuration
files - a RPconfig file which consists of values required to set up a pilot
on the target machine, a Kconfig file which consists of filenames/ parameter
values required by Amber/CoCo. The description of each of these parameters
is provided in their respective config files.

In this particular usecase example, there are 16 simulation instances followed
by 1 analysis instance forming one iteration. The experiment is run for two
such iterations. The output of the second iteration is stored on the local
machine under a folder called "backup".


.. code-block:: none

    [S]    [S]    [S]    [S]    [S]    [S]    [S]
     |      |      |      |      |      |      |
     \-----------------------------------------/
                          |
                         [A]
                          |
     /-----------------------------------------\
     |      |      |      |      |      |      |
    [S]    [S]    [S]    [S]    [S]    [S]    [S]
     |      |      |      |      |      |      |
     \-----------------------------------------/
                          |
                         [A]
                          :

Run Locally
^^^^^^^^^^^

.. warning:: In order to run this example, you need access to a MongoDB server and
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

.. warning:: Running locally would require you that have Amber and CoCo installed on
             your machine. Please go through Amber, CoCo documentation to see how this
             can be done.


By default, this example is setup to run on Stampede. You can also run it on your local
machine by setting the following parameters in your RPconfig file::

    REMOTE_HOST = 'localhost'
    UNAME       = ''
    ALLOCATION  = ''
    QUEUE       = ''
    WALLTIME    = 60
    PILOTSIZE   = 16
    WORKDIR     = None

    DBURL       = 'mongodb://extasy:extasyproject@extasy-db.epcc.ed.ac.uk/radicalpilot'


**Step 1:** View and download the example sources :ref:`below <01_static_amber_coco_loop.py>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python 01_static_amber_coco_loop.py --RPconfig stampede.rcfg --Kconfig cocoamber.wcfg

Once the script has finished running, you should see a folder called "iter2" inside backup/
which would contain 16 .ncdf files which are the output of the second simulation stage. You
see 16 .ncdf files since there were 16 simulation instances.

Run Remotely
^^^^^^^^^^^^

The script is configured to run on Stampede. You can increase the number
of cores to see how this affects the runtime of the script as the individual
simulations instances can run in parallel. You can try more variations
by modifying num_iterations(Kconfig), num_CUs (Kconfig), nsave (Kconfig), etc. ::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        project=None # add your allocation or project id here if required
    )

Once the default script has finished running, you should see a folder called "iter2" inside backup/
which would contain 16 .ncdf files which are the output of the second simulation stage. You
see 16 .ncdf files since there were 16 simulation instances.

.. _01_static_amber_coco_loop.py:

Example Source
^^^^^^^^^^^^^^
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
        k1.cores=2
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
            k1.download_output_data = ['md{0}.ncdf > backup/iter{0}/md_{0}_{1}.ncdf'.format(iteration,instance)]

        k2.cores = 2
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
                       "--output=pentaopt%s"%(iteration)]
        k1.cores = RPconfig.PILOTSIZE

        k1.link_input_data = ['$PRE_LOOP/{0}'.format(os.path.basename(Kconfig.top_file))]
        for iter in range(1,iteration+1):
            for i in range(1,Kconfig.num_CUs+1):
                k1.link_input_data = k1.link_input_data + ['$SIMULATION_ITERATION_{0}_INSTANCE_{1}/md{0}.ncdf > md_{0}_{1}.ncdf'.format(iter,i)]

        k1.copy_output_data = ['STDERR > $PRE_LOOP/STDERR']
        for i in range(0,Kconfig.num_CUs):
            k1.copy_output_data = k1.copy_output_data + ['pentaopt{0}{1}.pdb > $PRE_LOOP/pentaopt{0}{1}.pdb'.format(iteration,i)]


        k2 = Kernel(name="md.tleap")
        k2.arguments = ["--numofsims={0}".format(Kconfig.num_CUs),
                        "--cycle={0}".format(iteration)]

        k2.link_input_data = []
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

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
