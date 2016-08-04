md.amber
--------

Molecular Dynamics with Amber software package http://ambermd.org/

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --nwcoords=                | New coordinates file                                                             |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --instance=                | Instance number                                                                  |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --cycle=                   | Cycle number                                                                     |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --params=                  | Parameters file                                                                  |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --outfile=                 | Output file                                                                      |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --nwinfo=                  | New info file                                                                    |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --crdfile=                 | Input coordinate filename                                                        |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --nwtraj=                  | New trajectory file                                                              |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --mininfile=               | Input parameter filename                                                         |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --coords=                  | Coordinates file                                                                 |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --topfile=                 | Input topology filename                                                          |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --mdinfile=                | Input parameter filename                                                         |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: **xsede.stampede**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['/opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/sander']``
  * pre_exec: ``['module load TACC', 'module load amber/12.0']``

* Key: **lsu.supermic**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['sander']``
  * pre_exec: ``['. /home/vivek91/modules/amber14/amber.sh', 'export PATH=$PATH:/home/vivek91/modules/amber14/bin']``

* Key: **epsrc.archer**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['pmemd.MPI']``
  * pre_exec: ``['module load packages-archer', 'module load amber']``

* Key: *****

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``False``
  * executable: ``/bin/bash``
  * pre_exec: ``[]``

* Key: **xsede.comet**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['/opt/amber/bin/sander']``
  * pre_exec: ``['module load amber', 'module load python']``
