md.pre_grlsd_loop
-----------------

Splits the inputfile into 'numCUs' number of smaller files 

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --numCUs=                  | No. of files to be generated                                                     |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --inputfile=               | Input filename                                                                   |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: **xsede.stampede**

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``False``
  * executable: ``python``
  * pre_exec: ``['module load python', 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/apps/intel/15/composer_xe_2015.2.164/mkl/lib/intel64/:/opt/apps/intel/15/composer_xe_2015.2.164/compiler/lib/intel64/:/opt/apps/intel15/python/2.7.9/lib/']``

* Key: **lsu.supermic**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``python``
  * pre_exec: ``['module load python']``

* Key: **epsrc.archer**

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``False``
  * executable: ``python``
  * pre_exec: ``['module load python']``

* Key: *****

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``False``
  * executable: ``.``
  * pre_exec: ``[]``

* Key: **futuregrid.india**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``python``
  * pre_exec: ``['module load python']``
