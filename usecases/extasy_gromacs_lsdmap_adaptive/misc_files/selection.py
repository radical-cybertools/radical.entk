import os
import argparse
import numpy as np

from lsdmap.rw import reader


class SelectionStep(object):
    """
    SelectionStep()

    A class used to perform the selection step of Extended DM-d-MD 
    """

    def create_arg_parser(self):

        parser = argparse.ArgumentParser(description="select n new configurations uniformily along the first and second DCs..")
        parser.add_argument('npoints', metavar='n', type=int, help='number of configurations to be selected')

        # required options
        parser.add_argument("-s",
           type=str,
           dest="evfile",
           required=True,
           help="File containing the eigenvectors (DC's) of each configuration (input): ev")

        parser.add_argument("-o",
           type=str,
           dest="ncfile",
           required=False,
           help='File containing a single column with the number of copies to be made for each configuration (output, opt.): nc')

        return parser

   
    def run(self):

        parser = self.create_arg_parser()
        args = parser.parse_args()

        evfile = reader.open(args.evfile)
        evs = evfile.readlines()

        npoints = evs.shape[0]

        ev1s = evs[:,1]
        ev2s = evs[:,2]

        # minimum and maximum 1st DC values used
        min_ev1s = np.amin(ev1s)
        print "minimum probability at %.5f" %min_ev1s
        max_ev1s = np.amax(ev1s)
        print "maximum probability at %.5f" %max_ev1s

        # create a histogram with the values of the first DCs
        nbins = int(np.sqrt(npoints)/2)
        bins = min_ev1s + np.array(range(nbins+1)) * (max_ev1s - min_ev1s)/nbins
        # slightly change the size of the last bin to include max_ev1s
        bins[-1] += 1e-6
        # inds contains the idx of the bin of each point
        inds = np.digitize(ev1s, bins) - 1
        hist = [[] for i in range(nbins)]
        for idx, ind in enumerate(inds):
            hist[ind].append(idx) # create histogram with the idx of each point

        ncopiess = np.zeros(npoints, dtype='i')

        for idx in xrange(args.npoints):

            # sample a random number according to a uniform distribution between min_ev1s and max_ev1s
            sample = np.random.rand() * (max_ev1s - min_ev1s) + min_ev1s
            ind = np.digitize([sample], bins) - 1
            ind = ind[0] # find the index of the bin where the sample is located
            while len(hist[ind]) == 0: # while there is no endpoints in the bin...
                sample = np.random.rand() * (max_ev1s - min_ev1s) + min_ev1s # ... pick another sample
                ind = np.digitize([sample], bins) - 1
                ind = ind[0] # find the index of the bin where the sample is located

            # choose the new starting point in order to have a uniform distribution of points along the 2nd DC within the bin
            ev2s_bin = [ev2s[idx] for idx in hist[ind]]
            min_ev2s = np.amin(ev2s_bin)
            max_ev2s = np.amax(ev2s_bin)

            sample = np.random.rand() * (max_ev2s - min_ev2s) + min_ev2s
            idx_sample = (np.abs(ev2s_bin - sample)).argmin()
            jdx = hist[ind][idx_sample] 
            ncopiess[jdx] += 1

        if args.ncfile is None:
            args.ncfile = os.path.splitext(args.evfile)[0] + '.nc'
        with open(args.ncfile, 'w') as ncfile:
            for ncopies in ncopiess:
                print >> ncfile, ncopies

        print "number of copies stored in %s" %args.ncfile

if __name__ == '__main__':
    SelectionStep().run()
