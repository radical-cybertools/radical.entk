__author__ = 'vivek'

'''

Purpose :   This file merges the results of the simulation stage into one
            coordinate file as is required by the LSDMap analysis stage.
            This file reads from the simulation CU outputs and writes them
            into one common file in linear order.

Arguments : num_CUs = number of compute units
            md_output_file = name of the resulting output file
            path = path of the corresponding compute unit to read from

'''

import os
import sys

if __name__ == '__main__':
    num_CUs = int(sys.argv[1])
    md_output_file = sys.argv[2]
    path = sys.argv[3]

    with open(md_output_file, 'w') as output_grofile:
        for i in range(0,num_CUs):
            with open('%s/out%s.gro' % (path,i), 'r') as output_file:
                for line in output_file:
                    print >> output_grofile, line.replace("\n", "")
                    
    os.system('echo 2 | gmx trjconv -f tmp.gro -s tmp.gro -o tmpha.gro')
