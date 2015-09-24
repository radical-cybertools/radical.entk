md.namd
-------

The NAMD molecular dynamics toolkit (http://www.ks.uiuc.edu/Research/namd/)

**Arguments:**

This Kernel takes the same arguments and command-line parameters as the as the encapsulated tool.


**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: **stampede.tacc.utexas.edu**

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``/opt/apps/intel13/mvapich2_1_9/namd/2.9/bin/namd2``
  * pre_exec: ``['module load TACC && module load namd/2.9']``

* Key: *****

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``False``
  * executable: ``namd2``
  * pre_exec: ``['sleep 1']``

* Key: **supermuc.lrz.de**

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``/lrz/sys/applications/namd/2.9.1/mpi.ibm/NAMD_CVS-2013-11-11_Source/Linux-x86_64-icc/namd2``
  * pre_exec: ``['source /etc/profile.d/modules.sh', 'module load namd']``

* Key: **gordon.sdsc.xsede.org**

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``/opt/namd/2.9/bin/namd2``
  * pre_exec: ``['module load namd/2.9']``

* Key: **archer.ac.uk**

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``/usr/local/packages/namd/namd-2.9/bin/namd2``
  * pre_exec: ``['module load namd']``

* Key: **trestles.sdsc.xsede.org**

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``/opt/namd/bin/namd2``
  * pre_exec: ``['module load namd/2.9']``
