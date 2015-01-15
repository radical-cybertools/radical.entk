import sys
import os
import argparse
import linecache
import itertools as it
import numpy as np

from lsdmap.rw import reader
from lsdmap.rw import writer

class ReweightingStep(object):
    """
    ReweightingStep()

    A class used to perform the reweighting step of Extended DM-d-MD 
    """

    def initialize(self, args):

        # read structure file
        struct_file = reader.open(args.struct_file, velocities=True)
        self.struct_filename = struct_file.filename
        self.coords = struct_file.readlines()

        self.npoints = self.coords.shape[0]

        # read number of copies
        ncfile = reader.open(args.ncfile)
        self.ncopiess = ncfile.readlines() 

        # read wfile if exists

        if args.wfile is None:
            self.weights = np.ones(self.npoints, dtype='float')
        else:
            if os.path.isfile(args.wfile):
                wfile = reader.open(args.wfile)
                self.weights = wfile.readlines()
            else:
                self.weights = np.ones(self.npoints, dtype='float')

        assert self.ncopiess.size == self.npoints, "the number of lines of .nc file should be equal to the number\
                                                   of configurations in the structure file. %i/%i" %(self.ncopies.size, self.npoints)
        assert self.weights.size == self.npoints, "the number of lines of .w file should be equal to the number\
                                                   of configurations in the structure file. %i/%i" %(self.weights.size, self.npoints)

    def create_arg_parser(self):

        parser = argparse.ArgumentParser(description="save new configurations from .nc file (number of copies) \
                                         and the initial structure file and update the weights of each \
                                         configuration from the .nc file and the .w file, the configurations \
                                         will be saved in a structure file similar to the input one and \
                                         the new weights will be saved in a new .w file..")

        # required options
        parser.add_argument("-c",
            type=str,
            dest="struct_file",
            required=True,
            nargs='*',
            help = 'Structure file (input): gro, xvg')

        parser.add_argument("-n",
            type=str,
            dest="nnfile",
            required=True,
            help='File containing the indices of the nearest neighbors of all configurations of the structure file (input): nn')

        parser.add_argument("-s",
           type=str,
           dest="ncfile",
           required=True,
           help='File containing a single column with the number of copies to be made for each configuration (input): nc')

        parser.add_argument("-o",
            type=str,
            dest="output_file",
            required=True,
            nargs='*',
            help = 'Structure file (output): gro, xvg')

        # other options
        parser.add_argument("-w",
            type=str,
            dest="wfile",
            required=False,
            default='weight.w',
            help='File containing the weights of every point in a row (input/output, opt.): w')

        parser.add_argument("--max_alive_neighbors",
           type=int,
           dest="max_alive_neighbors",
           required=False,
           help='used to specify the maximum number of alive neighbors we want to consider when spreading the weight of a dead walker')

        parser.add_argument("--max_dead_neighbors",
           type=int,
           dest="max_dead_neighbors",
           required=False,
           help='used to specify the maximum number of dead neighbors for a dead walker to be removed, if the number of dead nneighbors is larger \
                 the specified number, it is kept!')

        return parser


    def get_nneighbors(self, nnfile_name, line):
        """
        read specific line of nnfile (use linecache instead of standard reader to avoid memory problems)
        """

        line = linecache.getline(nnfile_name, line+1)
        nneighbors = line.replace('\n','').split()
        nneighbors = map(int, nneighbors)
        nneighbors = np.array(nneighbors)

        return nneighbors

    def save(self, args):

        # save new coordinates
        format_output_file = os.path.splitext(args.output_file[0])[1]
        struct_file_writer = writer.open(format_output_file, pattern=args.struct_file[0])
        struct_file_writer.write(self.new_coords, args.output_file[0])

        # save wfile
        wfile_writer = writer.open('.w')
        wfile_writer.write(self.new_weights, args.wfile)

    def run(self):

        parser = self.create_arg_parser()
        args = parser.parse_args()

        self.initialize(args)

        dead_walkers_idxs = np.where(self.ncopiess==0)[0]
        print "number of dead walkers: %i" %dead_walkers_idxs.size
        ndead_walkers_kept = 0

        # look for dead walkers that are finally kept because their x first nneighbors are all dead walkers.
        for idx in dead_walkers_idxs:
            nneighbors = self.get_nneighbors(args.nnfile, idx)

            if args.max_dead_neighbors is None:
                nnneighbors = nneighbors.size
            else:
                nnneighbors = min(nneighbors.size, args.max_dead_neighbors + 1)

            if nnneighbors > 1: #the first element of nneighbors is always the point itself
                jdx = 1
                nneighbor_idx = nneighbors[jdx]
                while self.ncopiess[nneighbor_idx] == 0:
                    jdx += 1
                    if jdx == nnneighbors: # all the neighbors are dead walkers
                        break
                    else:
                        nneighbor_idx = nneighbors[jdx]
                if jdx == nnneighbors: # all nneighbors are dead within the range fixed
                    self.ncopiess[idx] = 1 # do not kill this walker 
                    ndead_walkers_kept += 1
            else: # the point has no nneighbor within the range fixed
                self.ncopiess[idx] = 1 # do not kill this walker 
                ndead_walkers_kept += 1

        print "number of dead walkers kept: %i" %ndead_walkers_kept
        # update dead_walkers_idxs
        if ndead_walkers_kept > 0:
            dead_walkers_idxs = np.where(self.ncopiess==0)[0]

        print "add weights of dead walkers to their nneighbor among new_walkers... "

        # redristribute the weights of dead walkers among their first alive nneighbors
        for idx in dead_walkers_idxs: # every idx has at least one nneighbor alive
            nneighbors = self.get_nneighbors(args.nnfile, idx)
            assert len(nneighbors) > 1, "the number of nnearest neighbors should be greater than 1."
            nneighbors = nneighbors[1:] # exclude the first neighbor which is the point itself
            nalive_nneighbors = 0
            if args.max_alive_neighbors is None: # if args.max_alive_neighbors is None, spread the weight of the dead walker over all their nearest neighbors available
                nalive_nneighbors = np.sum(self.ncopiess[nneighbors]) # number of neighbors alive
                self.weights[nneighbors] += self.weights[idx]*self.ncopiess[nneighbors]/nalive_nneighbors
            else: # if args.max_alive_neighbors > 0, spread the weight of dead walkers over the first "args.max_alive_neighbors" neighbors
                for jdx, nneighbor_idx in enumerate(nneighbors):
                    nalive_nneighbors += self.ncopiess[nneighbor_idx]
                    if nalive_nneighbors >= args.max_alive_neighbors:
                        break
                last_nneighbor_idx = jdx
                self.weights[nneighbors[:last_nneighbor_idx+1]] += self.weights[idx]*self.ncopiess[nneighbors[:last_nneighbor_idx+1]]/nalive_nneighbors
            self.weights[idx] = 0.0

        cutoff = 1e-6
        self.ncopiess[self.weights<cutoff] = 0        

        # build vector of new coords and new weights
        new_coords = []
        new_weights = []
        for ncopies, weight, coord in it.izip(self.ncopiess, self.weights, self.coords):
            if ncopies > 0:
                new_weight = weight/ncopies
                for ncopy in range(ncopies):
                    new_coords.append(coord)
                    new_weights.append(new_weight)

        self.new_coords = np.array(new_coords)
        self.new_weights = np.array(new_weights)

        sum_old_weights=int(round(np.sum(self.weights)))
        sum_new_weights=int(round(np.sum(self.new_weights)))
        assert sum_new_weights==sum_old_weights, \
            "sum of the weights differs from old number: sum_new_weights/sum_old_weights: %i/%i" %(sum_new_weights,sum_old_weights)

        self.save(args)

if __name__ == '__main__':
        ReweightingStep().run()