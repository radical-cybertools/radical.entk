#!/usr/bin/env python

__author__    = "Antons Treikalis <antons.treikalis@gmail.com>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import random

#-------------------------------------------------------------------------------

def get_historical_data(history_name):
    """
    """
    home_dir = os.getcwd()
    os.chdir("../")

    # getting all cu directories
    replica_dirs = []
    for name in os.listdir("."):
        if os.path.isdir(name):
            replica_dirs.append(name)    

    for directory in replica_dirs:
         os.chdir(directory)
         try:
             f = open(history_name)
             lines = f.readlines()
             f.close()
         except:
             pass 
         os.chdir("../")
 
    os.chdir(home_dir)
    value = (lines[0]).strip()
 
    return float(value) * random.uniform(1.1, 3.3)

#-------------------------------------------------------------------------------

if __name__ == '__main__':
    """This module calculates one swap matrix column for replica 
    """

    argument_list = str(sys.argv)
    replica_id = str(sys.argv[1])
    replica_cycle = str(sys.argv[2])
    replicas = int(str(sys.argv[3]))
    base_name = str(sys.argv[4])

    # getting output data for all replicas
    parameters = [0.0]*replicas
    for j in range(replicas):
        history_name = base_name + "_" + str(j) + "_" + replica_cycle + ".out" 
        try:
            rj_param = get_historical_data( history_name )
            parameters[j] = rj_param
        except:
             pass 

    #---------------------------------------------------------------------------
    # writing to file
    outfile = "matrix_column_{cycle}_{replica}.dat"\
    .format(cycle=replica_cycle, replica=replica_id )
    with open(outfile, 'w+') as f:
        row_str = ""
        for item in parameters:        
            if len(row_str) != 0:
                row_str = row_str + " " + str(item)
            else:
                row_str = str(item)   
        f.write(row_str)    
    f.close()
  
