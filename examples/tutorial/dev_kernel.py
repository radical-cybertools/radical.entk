from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {

    "name":         "amber",        #Mandatory
    "description":  "AMBER MD kernel",        #Optional
    "arguments":   {                #Mandatory
        "--minfile=": {
            "mandatory": True,        #Mandatory argument? True or False
            "description": "Minimization filename"
    	    },
        "--topfile=": {
            "mandatory": True,
            "description": "Topology filename"
                },
        "--crdfile=":{
            "mandatory": True,
            "description": "Coordinate filename"
                },
        "--output=":{
            "mandatory": True,
            "description": "Output filename"
                }
        },
    "machine_configs":             #Use a dictionary with keys as resource 
        {                                       #names and values specific to the resource
            "xsede.stampede":
            {
                "environment" : None,        #list or None, can be used to set env variables
                "pre_exec"    : ["module load TACC","module load amber/12.0"],            #list or None, can be used to load modules
                "executable"  : ["sander"],        #specify the executable to be used
                "uses_mpi"    : False            #mpi-enabled? True or False
            },
        }
}

# ------------------------------------------------------------------------------
#
class MyUserDefinedKernel(KernelBase):

    def __init__(self):

        super(MyUserDefinedKernel, self).__init__(_KERNEL_INFO)
     	"""Le constructor."""
        		
    # --------------------------------------------------------------------------
    #
    @staticmethod
    def get_name():
        return _KERNEL_INFO["name"]
        

    def _bind_to_resource(self, resource_key):
        """This function binds the Kernel to a specific resource defined in
        "resource_key".
        """        
        arguments = [
                           '-O',
                            '-i',self.get_arg("--minfile="),
                            '-o','min.out',
                            '-r',self.get_arg("--output="),
                            '-p',self.get_arg("--topfile="),
                            '-c',self.get_arg("--crdfile="),
                            '-ref',self.get_arg("--crdfile=")
                        ]

        self._executable  = _KERNEL_INFO["machine_configs"][resource_key]["executable"]
        self._arguments   = arguments
        self._environment = _KERNEL_INFO["machine_configs"][resource_key]["environment"]
        self._uses_mpi    = _KERNEL_INFO["machine_configs"][resource_key]["uses_mpi"]
        self._pre_exec    = _KERNEL_INFO["machine_configs"][resource_key]["pre_exec"]

# ------------------------------------------------------------------------------