md.gromacs
----------

Creates a new file of given size and fills it with random ASCII characters.

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --topol=                   | Topology filename                                                                |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --grompp=                  | Input parameter filename                                                         |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: **xsede.stampede**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['python']``
  * pre_exec: ``['module load intel/15.0.2', 'module load boost', 'module load gromacs', 'module load python']``

* Key: **lsu.supermic**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['python']``
  * pre_exec: ``['module load openmpi', 'module load python', 'export PATH=$PATH:/N/u/vivek91/modules/gromacs-5/bin']``

* Key: **epsrc.archer**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['python']``
  * pre_exec: ``['module load packages-archer', 'module load gromacs/5.0.0', 'module load python']``

* Key: *****

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``python``
  * pre_exec: ``[]``

* Key: **futuregrid.india**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['python']``
  * pre_exec: ``['module load openmpi', 'module load python', 'export PATH=$PATH:/N/u/vivek91/modules/gromacs-5/bin']``
