.. _custom_res:


Writing a Custom Resource Configuration File
================================================

If you want to use RADICAL-Pilot with a resource that is not in any of the provided resource configuration files, you can write your own, and drop it in $HOME/.radical/pilot/configs/<your_resource_configuration_file_name>.json.

.. note::  Be advised that you may need specific knowledge about the target resource to do so. Also, while RADICAL-Pilot can handle very different types of systems and batch system, it may run into trouble on specific configurations or software versions we did not encounter before. If you run into trouble using a resource not in our list of officially supported ones, please drop us a note on the RADICAL-Pilot users mailing list.

A configuration file has to be valid JSON. The structure is as follows:

.. code-block:: json

    # filename: lrz.json
    {
        "supermuc":
        {
            "description"                 : "The SuperMUC petascale HPC cluster at LRZ.",
            "notes"                       : "Access only from registered IP addresses.",
            "schemas"                     : ["gsissh", "ssh"],
            "ssh"                         :
            {
                "job_manager_endpoint"    : "loadl+ssh://supermuc.lrz.de/",
                "filesystem_endpoint"     : "sftp://supermuc.lrz.de/"
            },
            "gsissh"                      :
            {
                "job_manager_endpoint"    : "loadl+gsissh://supermuc.lrz.de:2222/",
                "filesystem_endpoint"     : "gsisftp://supermuc.lrz.de:2222/"
            },
            "default_queue"               : "test",
            "lrms"                        : "LOADL",
            "task_launch_method"          : "SSH",
            "mpi_launch_method"           : "MPIEXEC",
            "forward_tunnel_endpoint"     : "login03",
            "global_virtenv"              : "/home/hpc/pr87be/di29sut/pilotve",
            "pre_bootstrap_1"             : ["source /etc/profile",
                                             "source /etc/profile.d/modules.sh",
                                             "module load python/2.7.6",
                                             "module unload mpi.ibm", "module load mpi.intel",
                                             "source /home/hpc/pr87be/di29sut/pilotve/bin/activate"
                                        ],    
            "valid_roots"                    : ["/home", "/gpfs/work", "/gpfs/scratch"],
            "agent_type"                  : "multicore",
            "agent_scheduler"             : "CONTINUOUS",
            "aregent_spawner"               : "POPEN",
            "pilot_agent"                 : "radical-pilot-agent-multicore.py"
        },
        "ANOTHER_KEY_NAME":
        {
            ...
        }
    }

The name of your file (here lrz.json) together with the name of the resource (supermuc) form the resource key which is used in the class:ComputePilotDescription resource attribute (lrz.supermuc).

All fields are mandatory, unless indicated otherwise below.

* description: a human readable description of the resource.
* notes: information needed to form valid pilot descriptions, such as what parameter are required, etc.
* schemas: allowed values for the access_schema parameter of the pilot description. The first schema in the list is used by default. For each schema, a subsection is needed which specifies job_manager_endpoint and filesystem_endpoint.
* job_manager_endpoint: access url for pilot submission (interpreted by SAGA).
* filesystem_endpoint: access url for file staging (interpreted by SAGA).
* default_queue: queue to use for pilot submission (optional).
* lrms: type of job management system. Valid values are: LOADL, LSF, PBSPRO, SGE, SLURM, TORQUE, FORK.
* task_launch_method: type of compute node access, required for non-MPI units. Valid values are: SSH,`APRUN` or LOCAL.
* mpi_launch_method: type of MPI support, required for MPI units. Valid values are: MPIRUN, MPIEXEC, APRUN, IBRUN or POE.
* python_interpreter: path to python (optional).
* pre_bootstrap_1: list of commands to execute for initialization of main agent (optional).
* pre_bootstrap_2: list of commands to execute for initialization of sub-agent (optional).
* valid_roots: list of shared file system roots (optional). Note: pilot sandboxes must lie under these roots.
* pilot_agent: type of pilot agent to use. Currently: radical-pilot-agent-multicore.py.
* forward_tunnel_endpoint: name of the host which can be used to create ssh tunnels from the compute nodes to the outside world (optional).
