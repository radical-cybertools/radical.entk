from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {
    "name":         "sleep",        #Mandatory
    "description":  "sleeping kernel",        #Optional
    "arguments":   {                #Mandatory
        "--interval=": {
            "mandatory": True,        #Mandatory argument? True or False
            "description": "Number of seconds to do nothing."
    	    },
        }
    "machine_configs":             #Use a dictionary with keys as resource names and values specific to the resource
        {
            "local.localhost":
            {
                "environment" : None,        #list or None, can be used to set environment variables
                "pre_exec"    : None,            #list or None, can be used to load modules
                "executable"  : ["/bin/sleep"],        #specify the executable to be used
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
        		
    def _bind_to_resource(self, resource_key):
        """This function binds the Kernel to a specific resource defined in
        "resource_key".
        """        
        arguments  = ['{0}'.format(self.get_arg("--interval="))]

        self._executable  = _KERNEL_INFO["machine_configs"][resource_key]["executable"]
        self._arguments   = arguments
        self._environment = _KERNEL_INFO["machine_configs"][resource_key]["environment"]
        self._uses_mpi    = _KERNEL_INFO["machine_configs"][resource_key]["uses_mpi"]
        self._pre_exec    = _KERNEL_INFO["machine_configs"][resource_key]["pre_exec"]

# ------------------------------------------------------------------------------