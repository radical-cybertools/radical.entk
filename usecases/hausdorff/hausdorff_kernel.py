#!/usr/bin/env python

import sys, getopt,ast
import numpy as np
import argparse

def dH((P, Q)):
    def vsqnorm(v, axis=None):
        return np.sum(v*v, axis=axis)
    Ni = 3./P.shape[1]
    d = np.array([vsqnorm(pt - Q, axis=1) for pt in P])
    return ( max(d.min(axis=0).max(), d.min(axis=1).max())*Ni )**0.5


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--element_set1", help="The first Set of trajectories that will be used",nargs='*')
    parser.add_argument("--element_set2", help="The second set of trajectories that will be used",nargs='*')
    parser.add_argument("--output_file",help="File where the results will be written")
    args = parser.parse_args()

    set1 = args.element_set1    
    set2 = args.element_set2
    out_file = open(args.output_file,'w')

    trj_list1 = [np.load(i) for i in set1]

    if set2 != set1:
        trj_list2 = [np.load(i) for i in set2]
    else:
        trj_list2 = trj_list1


    for i in range(1,len(set1)+1):
        for j in range(1,len(set2)+1):
            comp=dH((trj_list1[i-1],trj_list2[j-1]))
            out_file.write('[{0},{1}] : {2}\n'.format(set1[i-1],set2[j-1],comp))

    out_file.close()