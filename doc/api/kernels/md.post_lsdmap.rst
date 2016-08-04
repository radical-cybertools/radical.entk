md.post_lsdmap
--------------

Creates a new file of given size and fills it with random ASCII characters.

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --max_alive_neighbors=     | Max alive neighbors to be considered                                             |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --numCUs=                  | No. of CUs                                                                       |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --out=                     | Output filename                                                                  |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --max_dead_neighbors=      | Max dead neighbors to be considered                                              |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --cycle=                   | Current iteration                                                                |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --num_runs=                | Number of runs to be generated in output file                                    |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: **xsede.stampede**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['python']``
  * pre_exec: ``['module load python', 'export PYTHONPATH=/home1/03036/jp43/.local/lib/python2.7/site-packages:$PYTHONPATH', 'export PYTHONPATH=/home1/03036/jp43/.local/lib/python2.7/site-packages/lsdmap/rw:$PYTHONPATH', 'export PYTHONPATH=/home1/03036/jp43/.local/lib/python2.7/site-packages/util:$PYTHONPATH']``

* Key: **epsrc.archer**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['python']``
  * pre_exec: ``['module load packages-archer', 'module load python']``

* Key: *****

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``.``
  * pre_exec: ``[]``

* Key: **futuregrid.india**

  * environment: ``{}``
  * uses_mpi: ``True``
  * executable: ``['python']``
  * pre_exec: ``['module load openmpi', 'module load python', 'export PATH=$PATH:/N/u/vivek91/.local/bin', 'export PYTHONPATH=$PYTHONPATH:/N/u/vivek91/.local/lib/python2.7/site-packages']``
