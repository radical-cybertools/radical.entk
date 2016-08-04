md.re_exchange
--------------

Calculates column of swap matrix for a given replica

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --replicas=                | number of replicas                                                               |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --new_temperature=         | temp                                                                             |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --replica_cycle=           | replica cycle                                                                    |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --calculator=              | name of python calculator file                                                   |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --replica_id=              | replica id                                                                       |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --replica_basename=        | name of base file                                                                |         0 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: **xsede.stampede**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['python']``
  * pre_exec: ``['module restore', 'module load python']``

* Key: **xsede.comet**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['python']``
  * pre_exec: ``['module load amber', 'module load python', 'module load mpi4py']``

* Key: *****

  * environment: ``None``
  * uses_mpi: ``False``
  * executable: ``python``
  * pre_exec: ``None``

* Key: **lsu.supermic**

  * environment: ``{}``
  * uses_mpi: ``False``
  * executable: ``['python']``
  * pre_exec: ``['module load python']``
