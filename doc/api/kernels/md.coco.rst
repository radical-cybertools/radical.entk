md.coco
-------

Creates a new file of given size and fills it with random ASCII characters.

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --frontpoints=             | No. of frontpoints = No. of simulation CUs                                       |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --dims=                    | No. of dimensions                                                                |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --output=                  | Output filename for postexec                                                     |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --topfile=                 | Topology filename                                                                |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --grid=                    | No. of grid points                                                               |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --mdfile=                  | NetCDF filename                                                                  |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: **xsede.stampede**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['python']``
  * pre_exec: ``['module load TACC', 'module load intel/13.0.2.146', 'module load python/2.7.9', 'module load netcdf/4.3.2', 'module load hdf5/1.8.13', 'export PYTHONPATH=/opt/apps/intel13/mvapich2_1_9/python/2.7.9/lib/python2.7/site-packages:/work/02998/ardi/coco-0.9_installation/lib/python2.7/site-packages:$PYTHONPATH', 'export PATH=/work/02998/ardi/coco-0.9_installation/bin:$PATH']``

* Key: **lsu.supermic**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['python']``
  * pre_exec: ``['module load hdf5/1.8.12/INTEL-140-MVAPICH2-2.0', 'module load netcdf/4.2.1.1/INTEL-140-MVAPICH2-2.0', 'module load fftw/3.3.3/INTEL-140-MVAPICH2-2.0', 'module load python/2.7.7-anaconda', 'export PATH=/home/vivek91/.local/bin:$PATH', 'export PYTHONPATH=/home/vivek91/.local/lib/python2.7/site-packages:$PYTHONPATH']``

* Key: **epsrc.archer**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['pyCoCo']``
  * pre_exec: ``['module load python', 'module load numpy', 'module load scipy', 'module load coco', 'module load netcdf4-python', 'module load amber']``

* Key: *****

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``pyCoCo``
  * pre_exec: ``[]``

* Key: **xsede.comet**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['python']``
  * pre_exec: ``['module load hdf5/1.8.14', 'module load netcdf/4.3.2', 'module load fftw/3.3.4', 'module load python', 'module load scipy', 'module load mpi4py', 'export PYTHONPATH=$PYTHONPATH:/home/vivek91/.local/lib/python2.7/site-packages']``
