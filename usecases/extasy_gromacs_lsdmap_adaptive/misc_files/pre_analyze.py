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
import glob

if __name__ == '__main__':
    md_output_file = sys.argv[1]
    path = sys.argv[2]

    with open(md_output_file, 'w') as output_grofile:
        for f in glob.glob('*.gro'):
            with open('%s/%s' % (path,f), 'r') as output_file:
                for line in output_file:
                    print >> output_grofile, line.replace("\n", "")
                    
    os.system('echo 2 | gmx trjconv -f tmp.gro -s tmp.gro -o tmpha.gro')
