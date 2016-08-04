md.mmpbsa
---------

MMPBSA.py - End-State Free Energy Calculations (http://pubs.acs.org/doi/abs/10.1021/ct300418h).

**Arguments:**

This Kernel takes the same arguments and command-line parameters as the as the encapsulated tool.


**Machine Configurations:**

Machine configurations describe specific configurations of the tool on a specific platform. ``*`` is a catch-all for all hosts for which no specific configuration exists.


* Key: *****

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``False``
  * executable: ``MMPBSA.py``
  * pre_exec: ``['sleep 1']``

* Key: **stampede.tacc.utexas.edu**

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``/opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/MMPBSA.py.MPI``
  * pre_exec: ``['module load python mpi4py amber']``

* Key: **archer.ac.uk**

  * environment: ``{'FOO': 'bar'}``
  * uses_mpi: ``True``
  * executable: ``//work/y07/y07/amber/12/bin/MMPBSA.py.MPI``
  * pre_exec: ``['module load amber']``
