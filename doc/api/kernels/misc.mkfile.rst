misc.mkfile
-----------

Creates a new file of given size and fills it with random ASCII characters.

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --size=                    | File size in bytes.                                                              |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --filename=                | Output filename.                                                                 |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: *****

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``False``
  * executable: ``base64``
  * pre_exec: ``[]``
