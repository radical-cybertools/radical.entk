md.lsdmap
---------

Creates a new file of given size and fills it with random ASCII characters.

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --config=                  | Config filename                                                                  |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: **xsede.stampede**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['/opt/apps/intel14/mvapich2_2_0/python/2.7.6/lib/python2.7/site-packages/mpi4py/bin/python-mpi']``
  * pre_exec: ``['module load -intel +intel/14.0.1.106', 'module load python', 'export PYTHONPATH=/home1/03036/jp43/.local/lib/python2.7/site-packages', 'export PATH=/home1/03036/jp43/.local/bin:$PATH']``

* Key: **epsrc.archer**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['python']``
  * pre_exec: ``['module load python', 'module load numpy', 'module load scipy', ' module load lsdmap', 'export PYTHONPATH=/work/y07/y07/cse/lsdmap/lib/python2.7/site-packages:$PYTHONPATH']``

* Key: *****

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``lsdmap``
  * pre_exec: ``[]``

* Key: **futuregrid.india**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['python']``
  * pre_exec: ``['module load openmpi', 'module load python', 'export PATH=$PATH:/N/u/vivek91/.local/bin', 'export PYTHONPATH=$PYTHONPATH:/N/u/vivek91/.local/lib/python2.7/site-packages']``
