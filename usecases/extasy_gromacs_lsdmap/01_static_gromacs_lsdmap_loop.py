#!/usr/bin/env python

"""

This example shows how to use the Ensemble MD Toolkit ``SimulationAnalysis``
pattern for the Gromacs-LSDMap usecase which has multiple Gromacs based Simulation
instances and a single LSDMap Analysis stage. Although the user is free to use
any method to mention the inputs, this usecase example uses two configuration
files - a RPconfig file (stampede.rcfg in this example) which consists of values
required to set up a pilot on the target machine, a Kconfig file (gromacslsdmap.wcfg
in this example) which consists of filenames/ parameter values required by
Gromacs/LSDMap. The description of each of these parameters is provided in their
respective config files.

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

.. warning:: Running locally would require you that have Gromacs and LSDMap installed on
             your machine. Please go through Gromacs, LSDMap documentation to see how this
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


Once the script has finished running, you should see a folder called "iter2" inside backup/
which would contain

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

**Step 1:** View and download the example sources :ref:`below <01_static_gromacs_lsdmap_loop.py>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python 01_static_gromacs_lsdmap_loop.py --RPconfig stampede.rcfg --Kconfig gromacslsdmap.wcfg


Once the default script has finished running, you should see a folder called "iter2" inside backup/
which would contain the coordinate file for the next iteration(out.gro), output log of lsdmap (lsdmap.log)
and the weight file (weight.w).

.. _01_static_gromacs_lsdmap_loop.py:

Example Source
^^^^^^^^^^^^^^
"""

__author__        = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__     = "Copyright 2014, http://radical.rutgers.edu"
__license__       = "MIT"
__use_case_name__ = "'Gromacs + LSDMap' simulation-analysis proof-of-concept (ExTASY)."
  
  
from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment
import sys
import imp
import argparse
import os
import pprint


# ------------------------------------------------------------------------------
#
class Gromacs_LSDMap(SimulationAnalysisLoop):
  # TODO Vivek: add description.

    def __init__(self, maxiterations, simulation_instances=1, analysis_instances=1):
        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)
    
    def pre_loop(self):
        '''
        function : transfers input files and intermediate executables

        pre_grlsd_loop :-
                Purpose : Transfers files, Split the input file into smaller files to be used by each of the
                            gromacs instances in the first iteration.

                Arguments : --inputfile = file to be split
                            --numCUs    = number of simulation instances/ number of smaller files
        '''
        k = Kernel(name="md.pre_grlsd_loop")
        k.upload_input_data = [Kconfig.md_input_file,
                               Kconfig.lsdm_config_file,
                               Kconfig.top_file,
                               Kconfig.mdp_file,
                               '{0}/spliter.py'.format(Kconfig.misc_loc),
                               '{0}/gro.py'.format(Kconfig.misc_loc),
                               '{0}/run.py'.format(Kconfig.misc_loc),
                               '{0}/pre_analyze.py'.format(Kconfig.misc_loc),
                               '{0}/post_analyze.py'.format(Kconfig.misc_loc),
                               '{0}/select.py'.format(Kconfig.misc_loc),
                               '{0}/reweighting.py'.format(Kconfig.misc_loc)]
        k.download_input_data = ['http://sourceforge.net/p/lsdmap/git/ci/extasy-0.1-rc2/tree/lsdmap/lsdm.py?format=raw > lsdm.py']
        k.arguments = ["--inputfile={0}".format(os.path.basename(Kconfig.md_input_file)),"--numCUs={0}".format(Kconfig.num_CUs)]
        return k

    def simulation_step(self, iteration, instance):

        '''
        function : In iter=1, use the input files from pre_loop, else use the outputs of the analysis stage in the
        previous iteration. Run gromacs in each instance using these files.

        gromacs :-

                Purpose : Run the gromacs simulation on each of the smaller files. Parameter files and executables are input
                            from pre_loop. There are 'numCUs' number of instances of gromacs per iteration.

                Arguments : --grompp    = gromacs parameters filename
                            --topol     = topology filename
        '''

        gromacs = Kernel(name="md.gromacs")
        gromacs.arguments = ["--grompp={0}".format(os.path.basename(Kconfig.mdp_file)),
                             "--topol={0}".format(os.path.basename(Kconfig.top_file))]
        gromacs.link_input_data = ['$PRE_LOOP/{0} > {0}'.format(os.path.basename(Kconfig.mdp_file)),
                                   '$PRE_LOOP/{0} > {0}'.format(os.path.basename(Kconfig.top_file)),
                                   '$PRE_LOOP/run.py > run.py']

        if (iteration-1==0):
            gromacs.link_input_data.append('$PRE_LOOP/temp/start{0}.gro > start.gro'.format(instance-1))

        else:
            gromacs.link_input_data.append('$ANALYSIS_ITERATION_{0}_INSTANCE_1/temp/start{1}.gro > start.gro'.format(iteration-1,instance-1))

        return gromacs
    
    def analysis_step(self, iteration, instance):
        '''
        function : Merge the results of each of the simulation instances and run LSDMap analysis to generate the
        new coordinate file. Split this new coordinate file into smaller files to be used by the simulation stage
        in the next iteration.

        If a step as multiple kernels (say k1, k2), data generated in k1 is implicitly moved to k2 (if k2 requires).
        Data which needs to be moved between the various steps (pre_loop, simulation_step, analysis_step) needs to
        be mentioned by the user.

        pre_lsdmap :-

                Purpose : The output of each gromacs instance in the simulation_step is a small coordinate file. Concatenate
                            such files from each of the gromacs instances to form a larger file. There is one instance of pre_lsdmap per
                            iteration.

                Arguments : --numCUs = number of simulation instances / number of small files to be concatenated

        lsdmap :-

                Purpose : Perform LSDMap on the large coordinate file to generate weights and eigen values. There is one instance
                            of lsdmap per iteration (MSSA : Multiple Simulation Single Analysis model).

                Arguments : --config = name of the config file to be used during LSDMap

        post_lsdmap :-


                Purpose : Use the weights, eigen values generated in lsdmap along with other parameter files from pre_loop
                            to generate the new coordinate file to be used by the simulation_step in the next iteration. There is one
                            instance of post_lsdmap per iteration.

                Arguments : --num_runs              = number of configurations to be generated in the new coordinate file
                            --out                   = output filename
                            --cycle                 = iteration number
                            --max_dead_neighbors    = max dead neighbors to be considered
                            --max_alive_neighbors   = max alive neighbors to be considered
                            --numCUs                = number of simulation instances/ number of smaller files
        '''

        pre_ana = Kernel(name="md.pre_lsdmap")
        pre_ana.arguments = ["--numCUs={0}".format(Kconfig.num_CUs)]
        pre_ana.link_input_data = ["$PRE_LOOP/pre_analyze.py > pre_analyze.py"]
        for i in range(1,Kconfig.num_CUs+1):
            pre_ana.link_input_data = pre_ana.link_input_data + ["$SIMULATION_ITERATION_{2}_INSTANCE_{0}/out.gro > out{1}.gro".format(i,i-1,iteration)]
        pre_ana.copy_output_data = ['tmpha.gro > $PRE_LOOP/tmpha.gro','tmp.gro > $PRE_LOOP/tmp.gro']

        lsdmap = Kernel(name="md.lsdmap")
        lsdmap.arguments = ["--config={0}".format(os.path.basename(Kconfig.lsdm_config_file))]
        lsdmap.link_input_data = ['$PRE_LOOP/{0} > {0}'.format(os.path.basename(Kconfig.lsdm_config_file)),'$PRE_LOOP/lsdm.py > lsdm.py','$PRE_LOOP/tmpha.gro > tmpha.gro']
        lsdmap.cores = RPconfig.PILOTSIZE
        if iteration > 1:
            lsdmap.link_input_data += ['$ANALYSIS_ITERATION_{0}_INSTANCE_1/weight.w > weight.w'.format(iteration-1)]
            lsdmap.copy_output_data = ['weight.w > $PRE_LOOP/weight.w']
        lsdmap.copy_output_data = ['tmpha.ev > $PRE_LOOP/tmpha.ev','out.nn > $PRE_LOOP/out.nn']
        
        if(iteration%Kconfig.nsave==0):
          lsdmap.download_output_data=['lsdmap.log > backup/iter{0}/lsdmap.log'.format(iteration)]

        post_ana = Kernel(name="md.post_lsdmap")
        post_ana.link_input_data = ["$PRE_LOOP/post_analyze.py > post_analyze.py",
                                    "$PRE_LOOP/select.py > select.py",
                                    "$PRE_LOOP/reweighting.py > reweighting.py",
                                    "$PRE_LOOP/spliter.py > spliter.py",
                                    "$PRE_LOOP/gro.py > gro.py",
                                    "$PRE_LOOP/tmp.gro > tmp.gro",
                                    "$PRE_LOOP/tmpha.ev > tmpha.ev",
                                    "$PRE_LOOP/out.nn > out.nn",
                                    "$PRE_LOOP/input.gro > input.gro"]

        post_ana.arguments = ["--num_runs={0}".format(Kconfig.num_runs),
                              "--out=out.gro",
                              "--cycle={0}".format(iteration-1),
                              "--max_dead_neighbors={0}".format(Kconfig.max_dead_neighbors),
                              "--max_alive_neighbors={0}".format(Kconfig.max_alive_neighbors),
                              "--numCUs={0}".format(Kconfig.num_CUs)]

        if iteration > 1:
            post_ana.link_input_data += ['$ANALYSIS_ITERATION_{0}_INSTANCE_1/weight.w > weight_new.w'.format(iteration-1)]

        if(iteration%Kconfig.nsave==0):
            post_ana.download_output_data = ['out.gro > backup/iter{0}/out.gro'.format(iteration),
                                             'weight.w > backup/iter{0}/weight.w'.format(iteration)]

        return [pre_ana,lsdmap,post_ana]


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

      # We set the 'instances' of the simulation step to 16. This means that 16
      # instances of the simulation are executed every iteration.
      # We set the 'instances' of the analysis step to 1. This means that only
      # one instance of the analysis is executed for each iteration
      randomsa = Gromacs_LSDMap(maxiterations=Kconfig.num_iterations, simulation_instances=Kconfig.num_CUs, analysis_instances=1)

      cluster.run(randomsa)

      pp = pprint.PrettyPrinter()

      pp.pprint(randomsa.execution_profile_dict)

      df = randomsa.execution_profile_dataframe
      df.to_pickle('exp_{0}_{1}.pkl'.format(RPconfig.PILOTSIZE,Kconfig.num_CUs))


  except EnsemblemdError, er:

    print "Ensemble MD Toolkit Error: {0}".format(str(er))
    raise # Just raise the execption again to get the backtrace
