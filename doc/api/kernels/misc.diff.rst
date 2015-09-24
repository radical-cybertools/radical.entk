misc.diff
---------

Counts the differences between two ASCII files.

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --outputfile=              | The output file containing the difference count.                                 |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --inputfile2=              | The second input ASCII file.                                                     |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --inputfile1=              | The first input ASCII file.                                                      |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: *****

  * environment: ``None``
  * uses_mpi: ``False``
  * executable: ``diff``
  * pre_exec: ``None``
