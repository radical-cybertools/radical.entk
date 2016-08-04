__author__ = 'vivek'

'''
Purpose :   This file is used to perform the selection + reweighting step required
            after the LSDMap stage to generate the new coordinate file. This file
            also splits this new coordinate file to be used by each of the compute
            units in the next iteration.

Arguments : num_runs        = number of initial configurations in the coordinate file
            evfile          = name of the eigen vector file
            num_clone_files = name of the clone file
            md_output_file  = output file of the pre_analyze step
            w_file          = name of the weight file
            outgrofile_name = filename of the output of select+reweight
            cycle           = current iteration number
            numCUs          = number of simulation instances

'''


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
    numCUs = int(sys.argv[12])

    os.system('python selection.py %s -s %s -o %s' %(num_runs,evfile,num_clone_files))
    #Update Boltzman weights

    try:
        #Remove old weight file
        os.remove('weight.w')

        #Rename new weight file
        os.rename('weight_new.w','weight.w')

    except:
        pass

    os.system('python reweighting.py -c %s -n %s -s %s -w %s -o %s --max_alive_neighbors=%s --max_dead_neighbors=%s' % (md_output_file,nearest_neighbor_file,num_clone_files,w_file,outgrofile_name,max_alive_neighbors,max_dead_neighbors))

    os.system('python spliter.py {0} {1}'.format(numCUs,outgrofile_name))

