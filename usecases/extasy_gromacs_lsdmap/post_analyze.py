__author__ = 'vivek'

import os
import sys

if __name__ =='__main__':
    num_runs = int(sys.argv[1])
    evfile = sys.argv[2]
    num_clone_files = sys.argv[3]
    md_output_file = sys.argv[4]
    nearest_neighbor_file = sys.argv[5]
    w_file = sys.argv[6]
    outgrofile_name = sys.argv[7]
    max_alive_neighbors = int(sys.argv[8])
    max_dead_neighbors = int(sys.argv[9])
    md_input_file = sys.argv[10]
    cycle = int(sys.argv[11])

    os.system('python select.py %s -s %s -o %s' %(num_runs,evfile,num_clone_files))
    #Update Boltzman weights

    os.system('python reweighting.py -c %s -n %s -s %s -w %s -o %s --max_alive_neighbors=%s --max_dead_neighbors=%s' % (md_output_file,nearest_neighbor_file,num_clone_files,w_file,outgrofile_name,max_alive_neighbors,max_dead_neighbors))

    #Rename outputfile as inputfile for next iteration
    os.system('mv %s %s_%s'%(outgrofile_name,cycle+1,md_input_file))
