
__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import radical.utils as ru
import sys
import time


# ------------------------------------------------------------------------------
#
def benchmark_pre (tid, app_cfg, bench_cfg) :

    if  not 'load' in app_cfg : 
        raise KeyError ('no load configured')


# ------------------------------------------------------------------------------
#
def benchmark_core (tid, i, app_cfg, bench_cfg) :

    time.sleep (float(app_cfg['load']))


# ------------------------------------------------------------------------------
#
def benchmark_post (tid, app_cfg, bench_cfg) :

    pass


# ------------------------------------------------------------------------------
#
cfg = sys.argv[1]
b = ru.Benchmark (cfg, 'job_run', benchmark_pre, benchmark_core, benchmark_post)
b.run  ()
b.eval ()


# ------------------------------------------------------------------------------


