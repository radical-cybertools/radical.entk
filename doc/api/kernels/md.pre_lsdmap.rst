md.pre_lsdmap
-------------

Creates a new file of given size and fills it with random ASCII characters.

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --numCUs=                  | No. of CUs                                                                       |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: **xsede.stampede**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['python']``
  * pre_exec: ``['module load intel/15.0.2', 'module load boost', 'module load gromacs', 'module load python', 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/apps/intel/15/composer_xe_2015.2.164/mkl/lib/intel64/:/opt/apps/intel/15/composer_xe_2015.2.164/compiler/lib/intel64/:/opt/apps/intel15/python/2.7.9/lib/']``

* Key: **epsrc.archer**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['/bin/bash']``
  * pre_exec: ``['module load packages-archer', 'module load gromacs/5.0.0', 'module load python']``

* Key: *****

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``.``
  * pre_exec: ``[]``

* Key: **futuregrid.india**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['python']``
  * pre_exec: ``['module load openmpi', 'module load python', 'export PATH=$PATH:/N/u/vivek91/modules/gromacs-5/bin:/N/u/vivek91/.local/bin']``
