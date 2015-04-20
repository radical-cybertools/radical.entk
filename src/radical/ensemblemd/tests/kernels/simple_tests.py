""" Tests cases
"""
import os
import sys
import glob
import unittest

import radical.ensemblemd

#-----------------------------------------------------------------------------
#
class SimpleKernelTests(unittest.TestCase):

    def setUp(self):
        # clean up fragments from previous tests
        pass

    def tearDown(self):
        # clean up after ourselves
        pass

    #-------------------------------------------------------------------------
    #
    def test__amber_kernel(self):
        """Basic test of the AMBER kernel.
        """
        k = radical.ensemblemd.Kernel(name="md.amber")
        k.arguments = ["--mininfile=abc", "--mdinfile=def", "--topfile=ghi","--cycle=1"]
        _kernel = k._bind_to_resource("*")
        assert type(_kernel) == radical.ensemblemd.kernel_plugins.md.amber.Kernel, _kernel

        # Test kernel specifics here:
        k = radical.ensemblemd.Kernel(name="md.amber")
        k.arguments = ["--mininfile=abc", "--mdinfile=def", "--topfile=ghi","--cycle=1"]

        k._bind_to_resource("*")
        assert k._cu_def_executable == "/bin/bash", k._cu_def_executable
        assert k.arguments == ['-l','-c','pmemd -O -i abc -o min1.out -inf min1.inf -r md1.crd -p ghi -c min1.crd -ref min1.crd && pmemd -O -i def -o md1.out -inf md1.inf -x md1.ncdf -r md1.rst -p ghi -c md1.crd'], k.arguments
        assert k._cu_def_pre_exec == [], k._cu_def_pre_exec
        assert k._cu_def_post_exec == None, k._cu_def_post_exec

        k._bind_to_resource("stampede.tacc.utexas.edu")
        assert k._cu_def_executable == "/bin/bash", k._cu_def_executable
        assert k.arguments == ['-l','-c','pmemd -O -i abc -o min1.out -inf min1.inf -r md1.crd -p ghi -c min1.crd -ref min1.crd && pmemd -O -i def -o md1.out -inf md1.inf -x md1.ncdf -r md1.rst -p ghi -c md1.crd'], k.arguments
        assert k._cu_def_pre_exec == ["module load TACC", "module load amber"], k._cu_def_pre_exec
        assert k._cu_def_post_exec == None, k._cu_def_post_exec

    #-------------------------------------------------------------------------
    #
    def test__coco_kernel(self):
        """Basic test of the CoCo kernel.
        """
        k = radical.ensemblemd.Kernel(name="md.coco")
        k.arguments = ["--grid=3", "--dims=3", "--frontpoints=8","--topfile=abc","--mdfile=def",
                       "--output=xyz","--cycle=1"]
        _kernel = k._bind_to_resource("*")
        assert type(_kernel) == radical.ensemblemd.kernel_plugins.md.coco.Kernel, _kernel

        # Test kernel specifics here:
        k = radical.ensemblemd.Kernel(name="md.coco")
        k.arguments = ["--grid=3", "--dims=3", "--frontpoints=8","--topfile=abc","--mdfile=def",
                       "--output=xyz","--cycle=1"]

        k._bind_to_resource("*")
        assert k._cu_def_executable == "/bin/bash", k._cu_def_executable
        assert k.arguments == ['-l','-c','pyCoCo --grid 3 --dims 3 --frontpoints 8 --topfile abc --mdfile def --output xyz && python postexec.py 8 1'], k.arguments
        assert k._cu_def_pre_exec == [], k._cu_def_pre_exec
        assert k._cu_def_post_exec == None, k._cu_def_post_exec

        k._bind_to_resource("stampede.tacc.utexas.edu")
        assert k._cu_def_executable == "/bin/bash", k._cu_def_executable
        assert k.arguments == ['-l','-c','pyCoCo --grid 3 --dims 3 --frontpoints 8 --topfile abc --mdfile def --output xyz && python postexec.py 8 1'], k.arguments
        assert k._cu_def_pre_exec == ["module load intel/13.0.2.146","module load python","module load netcdf/4.3.2",
                                      "module load hdf5/1.8.13","module load amber",
                                      "export PYTHONPATH=/work/02998/ardi/coco_installation/lib/python2.7/site-packages:$PYTHONPATH",
                                      "export PATH=/work/02998/ardi/coco_installation/bin:$PATH"], k._cu_def_pre_exec
        assert k._cu_def_post_exec == None, k._cu_def_post_exec


    #-------------------------------------------------------------------------
    #
    def test__gromacs_kernel(self):
        """Basic test of the GROMACS kernel.
        """
        k = radical.ensemblemd.Kernel(name="md.gromacs")
        k.arguments = ["--grompp=grompp.mdp","--topol=topol.top"]
        _kernel = k._bind_to_resource("*")
        assert type(_kernel) == radical.ensemblemd.kernel_plugins.md.gromacs.Kernel, _kernel

        # Test kernel specifics here:
        k = radical.ensemblemd.Kernel(name="md.gromacs")
        k.arguments = ["--grompp=grompp.mdp","--topol=topol.top"]

        k._bind_to_resource("*")
        assert k._cu_def_executable == "python", k._cu_def_executable
        assert k.arguments == ['run.py','--mdp','grompp.mdp','--gro','start.gro','--top','topol.top','--out','out.gro'], k.arguments
        assert k._cu_def_pre_exec == [], k._cu_def_pre_exec
        assert k._cu_def_post_exec == None, k._cu_def_post_exec

        k._bind_to_resource("stampede.tacc.utexas.edu")
        assert k._cu_def_executable == ["python"], k._cu_def_executable
        assert k.arguments == ['run.py','--mdp','grompp.mdp','--gro','start.gro','--top','topol.top','--out','out.gro'], k.arguments
        assert k._cu_def_pre_exec == ["module load gromacs python mpi4py"], k._cu_def_pre_exec
        assert k._cu_def_post_exec == None, k._cu_def_post_exec


    #-------------------------------------------------------------------------
    #
    def test__lsdmap_kernel(self):
        """Basic test of the LSDMAP kernel.
        """
        k = radical.ensemblemd.Kernel(name="md.lsdmap")
        k.arguments = ["--config=config.ini"]
        _kernel = k._bind_to_resource("*")
        assert type(_kernel) == radical.ensemblemd.kernel_plugins.md.lsdmap.Kernel, _kernel

        # Test kernel specifics here:
        k = radical.ensemblemd.Kernel(name="md.lsdmap")
        k.arguments = ["--config=config.ini"]

        k._bind_to_resource("*")
        assert k._cu_def_executable == "lsdmap", k._cu_def_executable

        assert k.arguments == ['lsdm.py', '-f','config.ini','-c','tmpha.gro','-n','out.nn','-w','weight.w'], k.arguments
        assert k._cu_def_pre_exec == [], k._cu_def_pre_exec
        assert k._cu_def_post_exec == None, k._cu_def_post_exec

        k._bind_to_resource("stampede.tacc.utexas.edu")
        assert k.arguments == ['lsdm.py', '-f','config.ini','-c','tmpha.gro','-n','out.nn','-w','weight.w'], k.arguments
        assert k._cu_def_post_exec == None, k._cu_def_post_exec
