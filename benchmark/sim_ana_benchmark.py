#!/usr/bin/env python

import os
import math
import shutil
import pprint
import tempfile
import numpy.lib
import numpy as np
import pandas as pd
import cPickle as pickle

from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment

# ------------------------------------------------------------------------------
# BENCHMARK PARAMETERS
#
config = {
    "idletime":           10,
    "cores":              [8],
    "instances":          [64],
    "instance_data_size": 10, # in MB (input / output symmteric)
    "iterations":         [4]
 }

def gen_datafile(size_in_mb):
    out = tempfile.NamedTemporaryFile(delete=False)
    out.seek((1024 * 1024 * size_in_mb) - 1)
    out.write('\0')
    out.close()
    return out.name

def save_pandas(fname, data):
    '''Save DataFrame or Series

    Parameters
    ----------
    fname : str
        filename to use
    data: Pandas DataFrame or Series
    '''
    np.save(open(fname, 'w'), data)
    if len(data.shape) == 2:
        meta = data.index,data.columns
    elif len(data.shape) == 1:
        meta = (data.index,)
    else:
        raise ValueError('save_pandas: Cannot save this type')
    s = pickle.dumps(meta)
    s = s.encode('string_escape')
    with open(fname, 'a') as f:
        f.seek(0, 2)
        f.write(s)

def load_pandas(fname, mmap_mode='r'):
    '''Load DataFrame or Series

    Parameters
    ----------
    fname : str
        filename
    mmap_mode : str, optional
        Same as numpy.load option
    '''
    values = np.load(fname, mmap_mode=mmap_mode)
    with open(fname) as f:
        numpy.lib.format.read_magic(f)
        numpy.lib.format.read_array_header_1_0(f)
        f.seek(values.dtype.alignment*values.size, 1)
        meta = pickle.loads(f.readline().decode('string_escape'))
    if len(meta) == 2:
        return pd.DataFrame(values, index=meta[0], columns=meta[1])
    elif len(meta) == 1:
        return pd.Series(values, index=meta[0])

# ------------------------------------------------------------------------------
#
class NopSA(SimulationAnalysisLoop):

    def __init__(self, maxiterations, simulation_instances, analysis_instances, idle_time, data_file_path, local_workdir):

        self.idle_time = idle_time
        self.data_file_path = data_file_path
        self.local_workdir = local_workdir

        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)

    def pre_loop(self):
        pass

    def simulation_step(self, iteration, instance):
        k = Kernel(name="misc.idle")
        k.arguments = ["--duration={0}".format(self.idle_time)]
        k.upload_input_data = ["%s > INPUT" % self.data_file_path]
        k.download_output_data = ["INPUT > %s" % self.local_workdir]
        return k

    def analysis_step(self, iteration, instance):
        k = Kernel(name="misc.idle")
        k.arguments = ["--duration={0}".format(self.idle_time)]
        k.upload_input_data = ["%s > INPUT" % self.data_file_path]
        k.download_output_data = ["INPUT > %s" % self.local_workdir]
        return k

    def post_loop(self):
        pass


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:

        # create a working directory
        local_workdir = tempfile.mkdtemp()
        print "Local workdir set to %s" % local_workdir
        data_file_path = gen_datafile(config["instance_data_size"])

        # iterate over cores
        for cfg_core in config["cores"]:

            # iterate over instances
            for cfg_inst in config["instances"]:

                for cfg_iter in config["iterations"]:

                    print "\n\ncores: %s instances: %s iterations: %s" % (cfg_core, cfg_inst, cfg_iter)

                    cluster = SingleClusterEnvironment(
                        resource="localhost",
                        cores=cfg_core,
                        walltime=30,
                        username=None,
                        allocation=None
                    )
                    # wait=True waits for the pilot to become active
                    # before the call returns. This is not useful when
                    # you want to take advantage of the queueing time /
                    # file-transfer overlap, but it's useful for baseline
                    # performance profiling of a specific pattern.
                    cluster.allocate(wait=True)

                    nopsa = NopSA(
                        maxiterations=cfg_iter,
                        simulation_instances=cfg_inst,
                        analysis_instances=cfg_inst,
                        idle_time = config["idletime"],
                        data_file_path = data_file_path,
                        local_workdir = local_workdir
                    )
                    cluster.run(nopsa)

                    pp = pprint.PrettyPrinter()
                    pp.pprint(nopsa.execution_profile_dict)

                    df = nopsa.execution_profile_dataframe
                    print df
                    df.to_pickle('result.pkl')

    except EnsemblemdError, er:
        print "Ensemble MD Toolkit Error: {0}".format(str(er))

    finally:
        os.remove(data_file_path)
        shutil.rmtree(local_workdir)
