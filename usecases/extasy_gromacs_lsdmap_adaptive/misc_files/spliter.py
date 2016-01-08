__author__ = 'vivek'

'''
Purpose :   This file is used to split the large input coordinate file into
            smaller coordinate files that can are executed on by each compute unit.
            It uses the gro.py file to get the specifics of the system in question.

Arguments : num_tasks = number of compute units
            grofile_name = name of the coordinate file

'''


import gro
import os
import sys
import shutil

if __name__ == '__main__':
    grofile_name = sys.argv[1]

    curdir = os.path.dirname(os.path.abspath(__file__))

    print 'Prepare grofiles..'

    grofile_obj = gro.GroFile(os.path.dirname(os.path.abspath(__file__)) + '/' + grofile_name)

    #nruns_per_task = [1 for _ in xrange(num_tasks)]
    #nextraruns=grofile_obj.nruns%num_tasks
    nruns_per_task = 1
    num_tasks = grofile_obj.nruns

    if os.path.isdir('%s/temp'%curdir) is True:
        shutil.rmtree('%s/temp' % curdir)
    os.mkdir('%s/temp'%curdir)

    with open(grofile_obj.filename, 'r') as grofile:
        for idx in xrange(num_tasks):
            start_grofile_name = curdir + '/temp/start%s.gro'%idx
            with open(start_grofile_name, 'w') as start_grofile:
                nlines_per_task = grofile_obj.nlines_per_run
                for jdx in xrange(nlines_per_task):
                    line=grofile.readline()
                    if line:
                        line = line.replace("\n", "")
                        print >> start_grofile, line
                    else:
                        break
