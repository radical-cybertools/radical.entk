import os
import sys
import linecache
import time

class GroFile(object):

    def __init__(self, filename):

        self.filename=filename
        self.natoms=self.get_natoms()
        self.nlines_per_run=self.natoms+3
        self.nlines=self.get_nlines()
        self.nruns=self.get_nruns()

    def get_natoms(self):
        natoms=int(linecache.getline(self.filename, 2))
        return natoms


    def get_nlines(self):
        with open(self.filename, 'r') as file:
            nlines = sum(1 for line in file)
        print nlines
        return nlines


    def get_nruns(self):
        assert self.nlines%(self.nlines_per_run)==0, "number of lines in %s is not a multiple of natoms+3" %self.filename
        nruns=self.nlines/self.nlines_per_run
        return nruns
