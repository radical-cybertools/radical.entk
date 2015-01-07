#!/usr/bin/env python
"""
TODO Vivek: Add description and instructions how to run, where to get
sample data from, etc. Refer to other use-cases for 'inspiration'.

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python 01_static_gromacs_lsdmap_loop.py
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


# ------------------------------------------------------------------------------
#
num_CUs = 4
class Gromacs_LSDMap(SimulationAnalysisLoop):
  # TODO Vivek: add description.

    def __init__(self, maxiterations, simulation_instances=1, analysis_instances=1):
        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)
    
    def pre_loop(self):
        k = Kernel(name="md.pre_grlsd_loop")
        k.upload_input_data = ['input.gro','config.ini','topol.top','grompp.mdp','spliter.py','gro.py','run.py','pre_analyze.py']
        return k

    def simulation_step(self, iteration, instance):
        '''TODO Vivek: add description of this step.
        '''
        pre_sim = Kernel(name="md.pre_gromacs")
        pre_sim.link_input_data = ["$PRE_LOOP/input.gro > input{0}.gro".format(iteration-1),"$PRE_LOOP/spliter.py","$PRE_LOOP/gro.py"]
        pre_sim.copy_input_data = ["$PRE_LOOP/spliter.py"]
        pre_sim.arguments = ["--inputfile=input{0}.gro".format(iteration-1),"--numCUs={0}".format(num_CUs)]

        gromacs = Kernel(name="md.gromacs")
        gromacs.arguments = ["--grompp=grompp.mdp","--topol=topol.top"]
        gromacs.link_input_data = ['$PRE_LOOP/grompp.mdp','$PRE_LOOP/topol.top','$PRE_LOOP/run.py']

        return [pre_sim, gromacs]
    
    def analysis_step(self, iteration, instance):
        '''TODO Vivek: add description of this step.
        '''

        pre_ana_1 = Kernel(name="md.pre_lsdmap_1")
        pre_ana_1.arguments = ["--numCUs={0}".format(num_CUs),"--out=out.gro"]
        pre_ana_1.copy_input_data = ["$PRE_LOOP/pre_analyze.py"]
        pre_ana_1.link_input_data = []
        for i in range(1,num_CUs+1):
            pre_ana_1.link_input_data = pre_ana_1.link_input_data + ["$PREV_SIMULATION_INSTANCE_{0}/out.gro > out{1}.gro".format(i,i-1)]

        pre_ana_2 = Kernel(name="md.pre_lsdmap_2")
        pre_ana_2.arguments = ["--out=out.gro"]

        return [pre_ana_1,pre_ana_2]


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

  try:
      # Create a new static execution context with one resource and a fixed
      # number of cores and runtime.
      cluster = SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=15,
        username='vivek91',
        allocation='TG-MCB090174'
      )

      # We set the 'instances' of the simulation step to 16. This means that 16
      # instances of the simulation are executed every iteration.
      # We set the 'instances' of the analysis step to 1. This means that only
      # one instance of the analysis is executed for each iteration
      randomsa = Gromacs_LSDMap(maxiterations=1, simulation_instances=num_CUs, analysis_instances=0)

      cluster.run(randomsa)
  
  except EnsemblemdError, er:

    print "EnsembleMD Error: {0}".format(str(er))
    raise # Just raise the execption again to get the backtrace
