md.tleap
--------

Creates a new file of given size and fills it with random ASCII characters.

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --cycle=                   | Output filename for postexec                                                     |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --numofsims=               | No. of frontpoints = No. of simulation CUs                                       |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: **xsede.stampede**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['python']``
  * pre_exec: ``['module load TACC', 'module load intel/13.0.2.146', 'module load python/2.7.9', 'module load amber', 'export PYTHONPATH=/opt/apps/intel13/mvapich2_1_9/python/2.7.9/lib/python2.7/site-packages:/work/02998/ardi/coco-0.9_installation/lib/python2.7/site-packages:$PYTHONPATH', 'export PATH=/work/02998/ardi/coco-0.6_installation/bin:$PATH']``

* Key: **lsu.supermic**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['python']``
  * pre_exec: ``['. /home/vivek91/modules/amber14/amber.sh', 'export PATH=/home/vivek91/.local/bin:/home/vivek91/modules/amber14/bin:/home/vivek91/modules/amber14/dat/leap/cmd:$PATH', 'export PYTHONPATH=/home/vivek91/.local/lib/python2.7/site-packages:$PYTHONPATH', 'module load hdf5/1.8.12/INTEL-140-MVAPICH2-2.0', 'module load netcdf/4.2.1.1/INTEL-140-MVAPICH2-2.0', 'module load fftw/3.3.3/INTEL-140-MVAPICH2-2.0', 'module load python/2.7.7-anaconda']``

* Key: **epsrc.archer**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['python']``
  * pre_exec: ``['module load python', 'module load numpy', 'module load scipy', 'module load coco', 'module load netcdf4-python', 'module load amber']``

* Key: *****

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``False``
  * executable: ``python``
  * pre_exec: ``[]``

* Key: **xsede.comet**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['python']``
  * pre_exec: ``['module load hdf5/1.8.14', 'module load netcdf/4.3.2', 'module load fftw/3.3.4', 'module load python', 'module load scipy', 'module load mpi4py', 'module load amber', 'export PYTHONPATH=$PYTHONPATH:/home/vivek91/.local/lib/python2.7/site-packages']``
