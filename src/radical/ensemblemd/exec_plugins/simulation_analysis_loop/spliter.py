__author__ = 'vivek'
import gro
import os
import sys
import shutil

if __name__ == '__main__':
    num_tasks = int(sys.argv[1])
    grofile_name = sys.argv[2]

    curdir = os.path.dirname(os.path.realpath(__file__))

    print 'Prepare grofiles..'

    grofile_obj = gro.GroFile(os.path.dirname(os.path.realpath(__file__)) + '/' + grofile_name)

    if grofile_obj.nruns<num_tasks:
        print "###ERROR: number of runs should be greater or equal to the number of tasks."
        sys.exit(1)

    nruns_per_task = [grofile_obj.nruns/num_tasks for _ in xrange(num_tasks)]
    nextraruns=grofile_obj.nruns%num_tasks

    for idx in xrange(nextraruns):
        nruns_per_task[idx] += 1

    if os.path.isdir('%s/temp'%curdir) is True:
        shutil.rmtree('%s/temp' % curdir)
    os.mkdir('%s/temp'%curdir)

    with open(grofile_obj.filename, 'r') as grofile:
        for idx in xrange(num_tasks):
            start_grofile_name = curdir + '/temp/start%s.gro'%idx
            with open(start_grofile_name, 'w') as start_grofile:
                nlines_per_task = nruns_per_task[idx]*grofile_obj.nlines_per_run
                for jdx in xrange(nlines_per_task):
                    line=grofile.readline()
                    if line:
                        line = line.replace("\n", "")
                        print >> start_grofile, line
                    else:
                        break
