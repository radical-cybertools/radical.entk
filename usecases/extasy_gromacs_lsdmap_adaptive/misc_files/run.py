__author__ = 'vivek'

'''
Purpose :   This file is executed by each of the compute units in the
            simulation stage. Uses the arguments to create a bash script
            which is executed on the remote machine to perform the
            gromacs simulation.

Arguments : --mdp = name of the gromacs parameter file
            --top = name of the topology file
            --gro = name of the coordinate file
            --out = name of the output file

'''


import os
import argparse
from mpi4py import MPI

def write_script(grofile_name,output_grofile_name,grompp_options,ndxfile_options,mdpfile_name,topfile_name,tprfile_name,size,mdrun_options,trrfile_name,edrfile_name):
    with open('run.sh','w') as file:
        script="""#!/bin/bash
        startgro=%(grofile_name)s
        tmpstartgro=tmpstart.gro
        outgro=%(output_grofile_name)s

        natoms=$(sed -n '2p' $startgro)
        nlines_per_frame=$((natoms+3))


        nlines=`wc -l $startgro| cut -d' ' -f1`
        nframes=$((nlines/nlines_per_frame))

        rm -rf $outgro

        for idx in `seq 1 $nframes`; do

            start=$(($nlines_per_frame*(idx-1)+1))
            end=$(($nlines_per_frame*idx))
            sed "$start"','"$end"'!d' $startgro > $tmpstartgro

            # gromacs preprocessing & MD
            gmx grompp %(grompp_options)s %(ndxfile_options)s -f %(mdpfile_name)s -c $tmpstartgro -p %(topfile_name)s -o %(tprfile_name)s
            gmx mdrun -nt %(size)s %(mdrun_options)s -s %(tprfile_name)s -o %(trrfile_name)s -e %(edrfile_name)s

            # store data
            cat confout.gro >> $outgro

        done
        rm -f $tmpstartgro
        """%locals()

        file.write(script)


if __name__ == '__main__':

    #initialize mpi variables

    comm = MPI.COMM_WORLD # MPI environment
    size = comm.Get_size() # number of threads
    rank = comm.Get_rank() # number of the current thread
    if rank==0:
        parser = argparse.ArgumentParser()
        parser.add_argument('--mdp',dest='mdpfile_name',required=True,type=str)
        parser.add_argument('--gro',dest='grofile_name',required=True,type=str)
        parser.add_argument('--top',dest='topfile_name',required=True,type=str)
        parser.add_argument('--out',dest='output_grofile_name',required=True,type=str)
        args = parser.parse_args()

        tprfile_name='topol.tpr'
        trrfile_name='traj.trr'
        edrfile_name='ener.edr'

        grompp_opts = os.environ.get('grompp_options','')
        mdrun_opts = os.environ.get('mdrun_options','')
        ndxfile_name = os.environ.get('ndxfile','')
        if ndxfile_name is not '':
            ndxfile_opts = '-n ' +ndxfile_name
        else:
            ndxfile_opts = ''

        write_script(args.grofile_name,args.output_grofile_name,grompp_opts,ndxfile_opts,args.mdpfile_name,args.topfile_name,tprfile_name,size,mdrun_opts,trrfile_name,edrfile_name)
        os.system('sh run.sh')

    else:
        pass

