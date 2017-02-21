#!/usr/bin/python 
import glob
import os
import pdb
import numpy as np

# figure out how many analyses have been done, make a new directory                                                            
dir = './'
analyses = glob.glob(os.path.join(dir,'results_*.txt'))
#analyses = sorted(analyses, key=lambda item: (int(item.partition('_')[2])))

gs = []
for a in analyses:
    f = open(a)
    lines = f.readlines()
    lastline = lines[len(lines)-1]
    # MBAR (for now) is #18
    vals = lastline.split()
    g = float(vals[16])
    dg = float(vals[18])
    gs.append(g)
    print "%10s %10.3f +/- %6.3f" % (a, g, dg)

gs = np.array(gs)
# now we can look at the standard deviation over the last 50% of the analyses
nanalyses = len(gs)
convergence_criteria = np.std(gs[nanalyses/2:])

# maybe use 0.1 kT?  Not sure if strict enough. It depends on the
# number of walkers -- the more walkers, the more analyses, the lower
# the standard deviation will be, because only the last set of data is
# changing.

print "%10.3f" % (convergence_criteria)