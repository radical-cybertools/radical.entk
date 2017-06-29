
.. _chapter_resources:

List of Pre-Configured Resources
================================


RESOURCE_STFC
-------------

JOULE_RUNJOB
************

The STFC Joule IBM BG/Q system (http://community.hartree.stfc.ac.uk/wiki/site/admin/home.html)

* **Resource label**      : ``stfc.joule_runjob``
* **Raw config**          : :download:`resource_stfc.json <resource_list/resource_stfc.json>`
* **Note**            : This currently needs a centrally administered outbound ssh tunnel.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : prod``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_LOCAL
--------------

LOCALHOST_SPARK_ANACONDA
************************

Your local machine gets spark.

* **Resource label**      : ``local.localhost_spark_anaconda``
* **Raw config**          : :download:`resource_local.json <resource_list/resource_local.json>`
* **Note**            : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : local``

* **Available schemas**   : ``local, ssh``

LOCALHOST_ORTELIB
*****************

Your local machine.

* **Resource label**      : ``local.localhost_ortelib``
* **Raw config**          : :download:`resource_local.json <resource_list/resource_local.json>`
* **Note**            : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : local``

* **Available schemas**   : ``local, ssh``

LOCALHOST_SPARK
***************

Your local machine gets spark.

* **Resource label**      : ``local.localhost_spark``
* **Raw config**          : :download:`resource_local.json <resource_list/resource_local.json>`
* **Note**            : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : local``

* **Available schemas**   : ``local, ssh``

LOCALHOST
*********

Your local machine.

* **Resource label**      : ``local.localhost``
* **Raw config**          : :download:`resource_local.json <resource_list/resource_local.json>`
* **Note**            : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : local``

* **Available schemas**   : ``local, ssh``

LOCALHOST_YARN
**************

Your local machine.

* **Resource label**      : ``local.localhost_yarn``
* **Raw config**          : :download:`resource_local.json <resource_list/resource_local.json>`
* **Note**            : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : local``

* **Available schemas**   : ``local, ssh``

LOCALHOST_ANACONDA
******************

Your local machine.

* **Resource label**      : ``local.localhost_anaconda``
* **Raw config**          : :download:`resource_local.json <resource_list/resource_local.json>`
* **Note**            : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : local``

* **Available schemas**   : ``local, ssh``

LOCALHOST_ORTE
**************

Your local machine.

* **Resource label**      : ``local.localhost_orte``
* **Raw config**          : :download:`resource_local.json <resource_list/resource_local.json>`
* **Note**            : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : local``

* **Available schemas**   : ``local, ssh``

RESOURCE_IU
-----------

BIGRED2_APRUN
*************

Indiana University's Cray XE6/XK7 cluster (https://kb.iu.edu/d/bcqt).

* **Resource label**      : ``iu.bigred2_aprun``
* **Raw config**          : :download:`resource_iu.json <resource_list/resource_iu.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

BIGRED2_CCM_SSH
***************

Indiana University's Cray XE6/XK7 cluster in Cluster Compatibility Mode (CCM) (https://kb.iu.edu/d/bcqt).

* **Resource label**      : ``iu.bigred2_ccm_ssh``
* **Raw config**          : :download:`resource_iu.json <resource_list/resource_iu.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : /N/dc2/scratch/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_XSEDE
--------------

BRIDGES
*******

The XSEDE 'Bridges' cluster at PSC (https://portal.xsede.org/psc-bridges/).

* **Resource label**      : ``xsede.bridges``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $HOME``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh, ssh, go``

BLACKLIGHT_SSH
**************

The XSEDE 'Blacklight' cluster at PSC (https://www.psc.edu/index.php/computing-resources/blacklight).

* **Resource label**      : ``xsede.blacklight_ssh``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

STAMPEDE_SPARK
**************

The XSEDE 'Stampede' cluster at TACC (https://www.tacc.utexas.edu/stampede/).

* **Resource label**      : ``xsede.stampede_spark``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $WORK``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh, ssh, go``

STAMPEDE_SSH
************

The XSEDE 'Stampede' cluster at TACC (https://www.tacc.utexas.edu/stampede/).

* **Resource label**      : ``xsede.stampede_ssh``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $WORK``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh, ssh, go``

STAMPEDE_YARN
*************

The XSEDE 'Stampede' cluster at TACC (https://www.tacc.utexas.edu/stampede/).

* **Resource label**      : ``xsede.stampede_yarn``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $WORK``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh, ssh, go``

COMET_SPARK
***********

The Comet HPC resource at SDSC 'HPC for the 99%' (http://www.sdsc.edu/services/hpc/hpc_systems.html#comet).

* **Resource label**      : ``xsede.comet_spark``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : compute``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

STAMPEDE_ORTE
*************

The XSEDE 'Stampede' cluster at TACC (https://www.tacc.utexas.edu/stampede/).

* **Resource label**      : ``xsede.stampede_orte``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $WORK``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh, go``

LONESTAR_SSH
************

The XSEDE 'Lonestar' cluster at TACC (https://www.tacc.utexas.edu/resources/hpc/lonestar).

* **Resource label**      : ``xsede.lonestar_ssh``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

WRANGLER_SSH
************

The XSEDE 'Wrangler' cluster at TACC (https://www.tacc.utexas.edu/wrangler/).

* **Resource label**      : ``xsede.wrangler_ssh``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $WORK``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh, go``

SUPERMIC_SPARK
**************

SuperMIC (pronounced 'Super Mick') is Louisiana State University's (LSU) newest supercomputer funded by the National Science Foundation's (NSF) Major Research Instrumentation (MRI) award to the Center for Computation & Technology. (https://portal.xsede.org/lsu-supermic)

* **Resource label**      : ``xsede.supermic_spark``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Partially allocated through XSEDE. Primary access through GSISSH. Allows SSH key authentication too.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : workq``
 * ``sandbox       : /work/$USER``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh, ssh``

GORDON_SSH
**********

The XSEDE 'Gordon' cluster at SDSC (http://www.sdsc.edu/us/resources/gordon/).

* **Resource label**      : ``xsede.gordon_ssh``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

COMET_ORTELIB
*************

The Comet HPC resource at SDSC 'HPC for the 99%' (http://www.sdsc.edu/services/hpc/hpc_systems.html#comet).

* **Resource label**      : ``xsede.comet_ortelib``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : compute``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

GREENFIELD
**********

The XSEDE 'Greenfield' cluster at PSC (https://www.psc.edu/index.php/computing-resources/greenfield).

* **Resource label**      : ``xsede.greenfield``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

COMET_SSH
*********

The Comet HPC resource at SDSC 'HPC for the 99%' (http://www.sdsc.edu/services/hpc/hpc_systems.html#comet).

* **Resource label**      : ``xsede.comet_ssh``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : compute``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

WRANGLER_SPARK
**************

The XSEDE 'Wrangler' cluster at TACC (https://www.tacc.utexas.edu/wrangler/).

* **Resource label**      : ``xsede.wrangler_spark``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $WORK``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh, ssh, go``

STAMPEDE_ORTELIB
****************

The XSEDE 'Stampede' cluster at TACC (https://www.tacc.utexas.edu/stampede/).

* **Resource label**      : ``xsede.stampede_ortelib``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail. To create a virtualenv for the first time, one needs to run towards the development queue.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $WORK``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh, go``

TRESTLES_SSH
************

The XSEDE 'Trestles' cluster at SDSC (http://www.sdsc.edu/us/resources/trestles/).

* **Resource label**      : ``xsede.trestles_ssh``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

COMET_ORTE
**********

The Comet HPC resource at SDSC 'HPC for the 99%' (http://www.sdsc.edu/services/hpc/hpc_systems.html#comet).

* **Resource label**      : ``xsede.comet_orte``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : compute``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

WRANGLER_YARN
*************

The XSEDE 'Wrangler' cluster at TACC (https://www.tacc.utexas.edu/wrangler/).

* **Resource label**      : ``xsede.wrangler_yarn``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : hadoop``
 * ``sandbox       : $WORK``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh, go``

SUPERMIC_SSH
************

SuperMIC (pronounced 'Super Mick') is Louisiana State University's (LSU) newest supercomputer funded by the National Science Foundation's (NSF) Major Research Instrumentation (MRI) award to the Center for Computation & Technology. (https://portal.xsede.org/lsu-supermic)

* **Resource label**      : ``xsede.supermic_ssh``
* **Raw config**          : :download:`resource_xsede.json <resource_list/resource_xsede.json>`
* **Note**            : Partially allocated through XSEDE. Primary access through GSISSH. Allows SSH key authentication too.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : workq``
 * ``sandbox       : /work/$USER``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh, ssh``

RESOURCE_EPSRC
--------------

ARCHER_ORTE
***********

The EPSRC Archer Cray XC30 system (https://www.archer.ac.uk/)

* **Resource label**      : ``epsrc.archer_orte``
* **Raw config**          : :download:`resource_epsrc.json <resource_list/resource_epsrc.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : standard``
 * ``sandbox       : /work/`id -gn`/`id -gn`/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

ARCHER_APRUN
************

The EPSRC Archer Cray XC30 system (https://www.archer.ac.uk/)

* **Resource label**      : ``epsrc.archer_aprun``
* **Raw config**          : :download:`resource_epsrc.json <resource_list/resource_epsrc.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : standard``
 * ``sandbox       : /work/`id -gn`/`id -gn`/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_DAS4
-------------

FS2_SSH
*******

The Distributed ASCI Supercomputer 4 (http://www.cs.vu.nl/das4/).

* **Resource label**      : ``das4.fs2_ssh``
* **Raw config**          : :download:`resource_das4.json <resource_list/resource_das4.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : all.q``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_NERSC
--------------

HOPPER_CCM_SSH
**************

The NERSC Hopper Cray XE6 in Cluster Compatibility Mode (https://www.nersc.gov/users/computational-systems/hopper/)

* **Resource label**      : ``nersc.hopper_ccm_ssh``
* **Raw config**          : :download:`resource_nersc.json <resource_list/resource_nersc.json>`
* **Note**            : For CCM you need to use special ccm_ queues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : ccm_queue``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

EDISON_ORTE
***********

The NERSC Edison Cray XC30 (https://www.nersc.gov/users/computational-systems/edison/)

* **Resource label**      : ``nersc.edison_orte``
* **Raw config**          : :download:`resource_nersc.json <resource_list/resource_nersc.json>`
* **Note**            : 
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : regular``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, go``

HOPPER_ORTE
***********

The NERSC Hopper Cray XE6 (https://www.nersc.gov/users/computational-systems/hopper/)

* **Resource label**      : ``nersc.hopper_orte``
* **Raw config**          : :download:`resource_nersc.json <resource_list/resource_nersc.json>`
* **Note**            : 
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : regular``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, go``

HOPPER_APRUN
************

The NERSC Hopper Cray XE6 (https://www.nersc.gov/users/computational-systems/hopper/)

* **Resource label**      : ``nersc.hopper_aprun``
* **Raw config**          : :download:`resource_nersc.json <resource_list/resource_nersc.json>`
* **Note**            : Only one CU per node in APRUN mode
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : regular``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

EDISON_CCM_SSH
**************

The NERSC Edison Cray XC30 in Cluster Compatibility Mode (https://www.nersc.gov/users/computational-systems/edison/)

* **Resource label**      : ``nersc.edison_ccm_ssh``
* **Raw config**          : :download:`resource_nersc.json <resource_list/resource_nersc.json>`
* **Note**            : For CCM you need to use special ccm_ queues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : ccm_queue``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

EDISON_APRUN
************

The NERSC Edison Cray XC30 (https://www.nersc.gov/users/computational-systems/edison/)

* **Resource label**      : ``nersc.edison_aprun``
* **Raw config**          : :download:`resource_nersc.json <resource_list/resource_nersc.json>`
* **Note**            : Only one CU per node in APRUN mode
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : regular``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, go``

RESOURCE_ORNL
-------------

TITAN_ORTE
**********

The Cray XK7 supercomputer located at the Oak Ridge Leadership Computing Facility (OLCF), (https://www.olcf.ornl.gov/titan/)

* **Resource label**      : ``ornl.titan_orte``
* **Raw config**          : :download:`resource_ornl.json <resource_list/resource_ornl.json>`
* **Note**            : Requires the use of an RSA SecurID on every connection.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $MEMBERWORK/`groups | cut -d' ' -f2```
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, local, go``

TITAN_ORTELIB
*************

The Cray XK7 supercomputer located at the Oak Ridge Leadership Computing Facility (OLCF), (https://www.olcf.ornl.gov/titan/)

* **Resource label**      : ``ornl.titan_ortelib``
* **Raw config**          : :download:`resource_ornl.json <resource_list/resource_ornl.json>`
* **Note**            : Requires the use of an RSA SecurID on every connection.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $MEMBERWORK/`groups | cut -d' ' -f2```
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, local, go``

TITAN_APRUN
***********

The Cray XK7 supercomputer located at the Oak Ridge Leadership Computing Facility (OLCF), (https://www.olcf.ornl.gov/titan/)

* **Resource label**      : ``ornl.titan_aprun``
* **Raw config**          : :download:`resource_ornl.json <resource_list/resource_ornl.json>`
* **Note**            : Requires the use of an RSA SecurID on every connection.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $MEMBERWORK/`groups | cut -d' ' -f2```
 * ``access_schema : local``

* **Available schemas**   : ``local, ssh, go``

RESOURCE_RICE
-------------

DAVINCI_SSH
***********

The DAVinCI Linux cluster at Rice University (https://docs.rice.edu/confluence/display/ITDIY/Getting+Started+on+DAVinCI).

* **Resource label**      : ``rice.davinci_ssh``
* **Raw config**          : :download:`resource_rice.json <resource_list/resource_rice.json>`
* **Note**            : DAVinCI compute nodes have 12 or 16 processor cores per node.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : parallel``
 * ``sandbox       : $SHARED_SCRATCH/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

BIOU_SSH
********

The Blue BioU Linux cluster at Rice University (https://docs.rice.edu/confluence/display/ITDIY/Getting+Started+on+Blue+BioU).

* **Resource label**      : ``rice.biou_ssh``
* **Raw config**          : :download:`resource_rice.json <resource_list/resource_rice.json>`
* **Note**            : Blue BioU compute nodes have 32 processor cores per node.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : serial``
 * ``sandbox       : $SHARED_SCRATCH/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_RADICAL
----------------

TWO
***

radical server 2

* **Resource label**      : ``radical.two``
* **Raw config**          : :download:`resource_radical.json <resource_list/resource_radical.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, local``

TUTORIAL
********

Our private tutorial VM on EC2

* **Resource label**      : ``radical.tutorial``
* **Raw config**          : :download:`resource_radical.json <resource_list/resource_radical.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, local``

ONE
***

radical server 1

* **Resource label**      : ``radical.one``
* **Raw config**          : :download:`resource_radical.json <resource_list/resource_radical.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, local``

RESOURCE_LRZ
------------

SUPERMUC_SSH
************

The SuperMUC petascale HPC cluster at LRZ, Munich (http://www.lrz.de/services/compute/supermuc/).

* **Resource label**      : ``lrz.supermuc_ssh``
* **Raw config**          : :download:`resource_lrz.json <resource_list/resource_lrz.json>`
* **Note**            : Default authentication to SuperMUC uses X509 and is firewalled, make sure you can gsissh into the machine from your registered IP address. Because of outgoing traffic restrictions your MongoDB needs to run on a port in the range 20000 to 25000.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : test``
 * ``sandbox       : $HOME``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh, ssh``

RESOURCE_NCSA
-------------

BW_LOCAL_ORTE
*************

The NCSA Blue Waters Cray XE6/XK7 system (https://bluewaters.ncsa.illinois.edu/)

* **Resource label**      : ``ncsa.bw_local_orte``
* **Raw config**          : :download:`resource_ncsa.json <resource_list/resource_ncsa.json>`
* **Note**            : Running 'touch .hushlogin' on the login node will reduce the likelihood of prompt detection issues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : /scratch/training/$USER``
 * ``access_schema : local``

* **Available schemas**   : ``local``

BW_APRUN
********

The NCSA Blue Waters Cray XE6/XK7 system (https://bluewaters.ncsa.illinois.edu/)

* **Resource label**      : ``ncsa.bw_aprun``
* **Raw config**          : :download:`resource_ncsa.json <resource_list/resource_ncsa.json>`
* **Note**            : Running 'touch .hushlogin' on the login node will reduce the likelihood of prompt detection issues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : /scratch/sciteam/$USER``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh``

BW_ORTELIB
**********

The NCSA Blue Waters Cray XE6/XK7 system (https://bluewaters.ncsa.illinois.edu/)

* **Resource label**      : ``ncsa.bw_ortelib``
* **Raw config**          : :download:`resource_ncsa.json <resource_list/resource_ncsa.json>`
* **Note**            : Running 'touch .hushlogin' on the login node will reduce the likelihood of prompt detection issues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : /scratch/sciteam/$USER``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh``

BW_CCM_SSH
**********

The NCSA Blue Waters Cray XE6/XK7 system in CCM (https://bluewaters.ncsa.illinois.edu/)

* **Resource label**      : ``ncsa.bw_ccm_ssh``
* **Raw config**          : :download:`resource_ncsa.json <resource_list/resource_ncsa.json>`
* **Note**            : Running 'touch .hushlogin' on the login node will reduce the likelihood of prompt detection issues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : /scratch/sciteam/$USER``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh``

BW_ORTE
*******

The NCSA Blue Waters Cray XE6/XK7 system (https://bluewaters.ncsa.illinois.edu/)

* **Resource label**      : ``ncsa.bw_orte``
* **Raw config**          : :download:`resource_ncsa.json <resource_list/resource_ncsa.json>`
* **Note**            : Running 'touch .hushlogin' on the login node will reduce the likelihood of prompt detection issues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : /scratch/sciteam/$USER``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh``

RESOURCE_NCAR
-------------

YELLOWSTONE_SSH
***************

The Yellowstone IBM iDataPlex cluster at UCAR (https://www2.cisl.ucar.edu/resources/yellowstone).

* **Resource label**      : ``ncar.yellowstone_ssh``
* **Raw config**          : :download:`resource_ncar.json <resource_list/resource_ncar.json>`
* **Note**            : We only support one concurrent CU per node currently.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : premium``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_FUTUREGRID
-------------------

DELTA_SSH
*********

FutureGrid Supermicro GPU cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.delta_ssh``
* **Raw config**          : :download:`resource_futuregrid.json <resource_list/resource_futuregrid.json>`
* **Note**            : Untested.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : delta``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

XRAY_APRUN
**********

FutureGrid Cray XT5m cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.xray_aprun``
* **Raw config**          : :download:`resource_futuregrid.json <resource_list/resource_futuregrid.json>`
* **Note**            : One needs to add 'module load torque' to ~/.profile on xray.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : /scratch/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

INDIA_SSH
*********

The FutureGrid 'india' cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.india_ssh``
* **Raw config**          : :download:`resource_futuregrid.json <resource_list/resource_futuregrid.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

XRAY_CCM
********

FutureGrid Cray XT5m cluster in Cluster Compatibility Mode (CCM) (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.xray_ccm``
* **Raw config**          : :download:`resource_futuregrid.json <resource_list/resource_futuregrid.json>`
* **Note**            : One needs to add 'module load torque' to ~/.profile on xray.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : ccm_queue``
 * ``sandbox       : /scratch/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

ECHO_SSH
********

FutureGrid Supermicro ScaleMP cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.echo_ssh``
* **Raw config**          : :download:`resource_futuregrid.json <resource_list/resource_futuregrid.json>`
* **Note**            : Untested
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : echo``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

BRAVO_SSH
*********

FutureGrid Hewlett-Packard ProLiant compute cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.bravo_ssh``
* **Raw config**          : :download:`resource_futuregrid.json <resource_list/resource_futuregrid.json>`
* **Note**            : Works only up to 64 cores, beyond that Torque configuration is broken.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : bravo``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

