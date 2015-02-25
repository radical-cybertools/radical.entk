#!/usr/bin/env python

"""
.. module:: radical.repex.namd_kernels.matrix_calculator_scheme_2
.. moduleauthor::  <antons.treikalis@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import sys

#-----------------------------------------------------------------------------------------------------------------------------------

def reduced_energy(temperature, potential):
    """Calculates reduced energy.

    Arguments:
    temperature - replica temperature
    potential - replica potential energy

    Returns:
    reduced energy of replica
    """
    kb = 0.0019872041
    # check for division by zero
    if temperature != 0:
        beta = 1. / (kb*temperature)
    else:
        beta = 1. / kb     
    return float(beta * potential)

#-----------------------------------------------------------------------------------------------------------------------------------

def get_historical_data(history_name):
    """Retrieves temperature and potential energy from simulation output file .history file.
    This file is generated after each simulation run. The function searches for directory 
    where .history file resides by checking all ComputeUnit directories on target resource.

    Arguments:
    history_name - name of .history file for a given replica. 

    Returns:
    data[0] - temperature obtained from .history file
    data[1] - potential energy obtained from .history file
    path_to_replica_folder - path to ComputeUnit directory on a target resource where all
    input/output files for a given replica recide.
    """
    home_dir = os.getcwd()
    os.chdir("../")
    
    os.chdir("staging_area")
    try:
        f = open(history_name)
        lines = f.readlines()
        f.close()
        path_to_replica_folder = os.getcwd()
        data = lines[0].split()
    except:
        pass 
    os.chdir("../")
 
    os.chdir(home_dir)
    return float(data[0]), float(data[1]), path_to_replica_folder

#-----------------------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    """This module calculates one swap matrix column for a single replica, 
    which is then transferred back to RE application using STDOUT redirestion 
    capabilities of Radical Pilot.
    """

    argument_list = str(sys.argv)
    replica_id = str(sys.argv[1])
    replica_cycle = str(sys.argv[2])
    replicas = int(str(sys.argv[3]))
    base_name = str(sys.argv[4])

    pwd = os.getcwd()

    # getting history data for self
    history_name = base_name + "_" + replica_id + "_" + replica_cycle + ".history"
    replica_temp, replica_energy, path_to_replica_folder = get_historical_data( history_name )

    # getting history data for all replicas
    # we rely on the fact that last cycle for every replica is the same, e.g. == replica_cycle
    # but this is easily changeble for arbitrary cycle numbers
    temperatures = [0.0]*replicas
    energies = [0.0]*replicas
    for j in range(replicas):
        history_name = base_name + "_" + str(j) + "_" + replica_cycle + ".history" 
        try:
            rj_temp, rj_energy, temp = get_historical_data( history_name )
            temperatures[j] = rj_temp
            energies[j] = rj_energy
        except:
             pass 

    # init swap column
    swap_column = [0.0]*replicas

    for j in range(replicas):        
        swap_column[j] = reduced_energy(temperatures[j], replica_energy)


    for item in swap_column:
        print item,    
        
    # printing path
    print str(path_to_replica_folder).rstrip()

    print str(replica_id)
    
     
