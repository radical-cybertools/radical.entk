from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {

    "name":         "amber",        
    "description":  "AMBER MD kernel",        
    "arguments":   {                
    	#Needs to be filled by user
        },
    "machine_configs":             
        {                                      
        	#Needs to be filled by user

        }
}

# ------------------------------------------------------------------------------


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
 		#Needs to be filled by user
                        ]

        self._executable  = _KERNEL_INFO["machine_configs"][resource_key]["executable"]
        self._arguments   = arguments
        self._environment = _KERNEL_INFO["machine_configs"][resource_key]["environment"]
        self._uses_mpi    = _KERNEL_INFO["machine_configs"][resource_key]["uses_mpi"]
        self._pre_exec    = _KERNEL_INFO["machine_configs"][resource_key]["pre_exec"]

# ------------------------------------------------------------------------------