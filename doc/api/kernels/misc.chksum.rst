misc.chksum
-----------

Calculates an SHA1 checksum for a given file.

**Arguments:**

+----------------------------+----------------------------------------------------------------------------------+-----------+
| Argument Name              | Description                                                                      | Mandatory |
+============================+==================================================================================+===========+
| --outputfile=              | The output file containing SHA1 sum.                                             |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+
| --inputfile=               | The input file.                                                                  |         1 |
+----------------------------+----------------------------------------------------------------------------------+-----------+

**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: *****

  * environment: ``None``
  * uses_mpi: ``False``
  * executable: ``$SHASUM``
  * pre_exec: ``['command -v sha1sum >/dev/null 2>&1 && export SHASUM=sha1sum  || export SHASUM=shasum']``
