__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *

class Kernel(object):

    def __init__(self, name=None):

        self._name = name

        # Parameters required for any Kernel irrespective of RP
        self._pre_exec                  = list()
        self._post_exec                 = list()
        self._executable                = str()
        self._arguments                 = list()
        self._mpi                  = False
        self._cores                     = 1 # If unspecified, number of cores is set to 1

        self._upload_input_data          = list()
        self._link_input_data            = list()
        self._download_output_data       = list()
        self._copy_input_data            = list()
        self._copy_output_data           = list()

        self._machine_config            = dict()

        self._logger = ru.get_logger("radical.entk.Kernel")

    #  ------------------------------------------------------------- 
    
    def as_dict(self):
        """Returns a dictionary representation of the kernel"""
    
        kernel_dict =   {     
                            "pre_exec":     self._pre_exec,
                            "executable":   self._executable,
                            "arguments":    self._arguments,
                            "mpi":          self._mpi,
                            "cores":        self._cores
                        }

        return kernel_dict


    # -------------------------------------------------------------
    @property
    def name(self):
        return self._name
    
    # -------------------------------------------------------------
    # Methods to get via API

    
    # -------------------------------------------------------------
    # -------------------------------------------------------------
    # Methods to set kernel parameters via API

    # executable
    # pre_exec
    # post_exec
    # arguments
    # cores
    # upload_input_data
    # link_input_data
    # copy_input_data
    # download_output_data
    # copy_output_data
    
    #timeout

    # -------------------------------------------------------------
    
    @property
    def executable(self):
        return self._executable

    @executable.setter
    def executable(self, executable):
        if type(executable) != str:
            raise TypeError(
                expected_type=str,
                actual_type=type(executable))

        self._executable = executable
    # -------------------------------------------------------------

    @property
    def pre_exec(self):
        return self._pre_exec

    @pre_exec.setter
    def pre_exec(self, pre_exec):

        if type(pre_exec) != list:
            raise TypeError(
                expected_type=list,
                actual_type=type(pre_exec))

        self._pre_exec = pre_exec
    # -------------------------------------------------------------

    @property
    def post_exec(self):
        return self._post_exec

    @post_exec.setter
    def post_exec(self, post_exec):

        if type(post_exec) != list:
            raise TypeError(
                expected_type=list,
                actual_type=type(post_exec))

        self._post_exec = post_exec
    # -------------------------------------------------------------

    @property
    def mpi(self):
        return self._mpi

    @mpi.setter
    def mpi(self, mpi):

        if type(mpi) != bool:
            raise TypeError(
                expected_type=bool,
                actual_type=type(mpi))

        # Call the validate_args() method of the plug-in.
        self._mpi = mpi
    # -------------------------------------------------------------

    @property
    def arguments(self):
        """List of arguments to the kernel as defined by the kernel definition files"""
        return self._arguments

    @arguments.setter
    def arguments(self, args):
        """Sets the arguments for the kernel.
        """
        if type(args) != list:
            raise TypeError(
                expected_type=list,
                actual_type=type(args))

        self._arguments = args
    # -------------------------------------------------------------

    @property
    def cores(self):
        """The number of cores the kernel is using.
        """
        return self._cores

    @cores.setter
    def cores(self, cores):

        if type(cores) != int:
            raise TypeError(
                expected_type=int,
                actual_type=type(cores))

        self._cores = cores
    # -------------------------------------------------------------

    @property
    def upload_input_data(self):
        return self._upload_input_data

    @upload_input_data.setter
    def upload_input_data(self, data_directives):
        if type(data_directives) != list:
            data_directives = [data_directives]

        for dd in data_directives:
            if type(dd) != str:
                raise TypeError(
                    expected_type=str,
                    actual_type=type(dd))

        self._upload_input_data = data_directives
    # -------------------------------------------------------------

    @property
    def link_input_data(self):
        return self._link_input_data

    @link_input_data.setter
    def link_input_data(self, data_directives):
        if type(data_directives) != list:
            data_directives = [data_directives]

        for dd in data_directives:
            if type(dd) != str:
                raise TypeError(
                    expected_type=str,
                    actual_type=type(dd))

        self._link_input_data = data_directives
    # -------------------------------------------------------------

    @property
    def copy_input_data(self):
        return self._copy_input_data

    @copy_input_data.setter
    def copy_input_data(self, data_directives):

        if type(data_directives) != list:
            data_directives = [data_directives]

        for dd in data_directives:
            dd = str(dd)
            if type(dd) != str:
                raise TypeError(
                    expected_type=str,
                    actual_type=type(dd))

        self._copy_input_data = data_directives
    # -------------------------------------------------------------

    @property
    def download_output_data(self):
        return self._download_output_data

    @download_output_data.setter
    def download_output_data(self, data_directives):

        if type(data_directives) != list:
            data_directives = [data_directives]

        for dd in data_directives:
            if type(dd) != str:
                raise TypeError(
                    expected_type=str,
                    actual_type=type(dd))

        self._download_output_data = data_directives
    # -------------------------------------------------------------

    @property
    def copy_output_data(self):
        return self._copy_output_data

    @copy_output_data.setter
    def copy_output_data(self, data_directives):

        if type(data_directives) != list:
            data_directives = [data_directives]

        for dd in data_directives:
            if type(dd) != str:
                raise TypeError(
                    expected_type=str,
                    actual_type=type(dd))

        self._copy_output_data = data_directives
    # -------------------------------------------------------------
    def _bind_to_resource(self, resource):

        try:

            if (not self._pre_exec)and(self._machine_config):
                self._pre_exec = self._machine_config[resource]["pre_exec"]
            if (not self._executable)and(self._machine_config):
                self._executable = self._machine_config[resource]["executable"]
            
        except Exception, ex:
            self._logger.error('Kernel bind to resource failed, error: %s'%ex)
            raise
    # -------------------------------------------------------------

    @property
    def machine_config(self):
        return self._machine_config
    
    @machine_config.setter
    def machine_config(self, config):
        self._machine_config = config
    # -------------------------------------------------------------

    def _validate_config(self, resource_name, kernel_base):

        '''
        Config needs to be a dictionary with values for 'pre_exec'
        and 'executable'
        '''

        self._machine_config = kernel_base.kernel_info['machine_configs']

        if self._machine_config:

            if resource_name in self._machine_config:

                if "pre_exec" not in self._machine_config[resource_name]:
                    raise MissingValueError("no pre_exec in config for %s"%mach_name)

                if "executable" not in self._machine_config[resource_name]:
                    raise MissingValueError("no executable in config for %s"%mach_name)


        kernel_base.arguments = self._arguments
        kernel_base.validate_arguments()
        kernel_base._bind_to_resource(resource_name)

        self._pre_exec = kernel_base.pre_exec

        if not self._executable:
            self._executable = kernel_base.executable
            
        self._arguments = kernel_base.arguments

        