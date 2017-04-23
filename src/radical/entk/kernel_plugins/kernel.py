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
        self._uses_mpi                  = False
        self._cores                     = 1 # If unspecified, number of cores is set to 1

        self._upload_input_data          = list()
        self._link_input_data            = list()
        self._download_input_data        = list()
        self._download_output_data       = list()
        self._copy_input_data            = list()
        self._copy_output_data           = list()

        self._logger = ru.get_logger("radical.entk.Kernel")

        # Variables to keep it compatible with existing scripts -- this will be removed in the 
        # next version.
        self._raw_args                  = None
        self._machine_configs           = None
        self._valid_args                = dict()
        self._binding_function          = None


    #  ------------------------------------------------------------- 
    
    def as_dict(self):
        """Returns a dictionary representation of the kernel"""
    
        kernel_dict =   {     
                            "pre_exec":     self._pre_exec,
                            "executable":   self._executable,
                            "arguments":    self._arguments,
                            "uses_mpi":     self._uses_mpi,
                            "cores":        self._cores
                        }

        return kernel_dict


    # -------------------------------------------------------------
    @property
    def name(self):
        return self._name
    
    def get_name():
        return self._name

    # -------------------------------------------------------------
    # Methods to get via API

    
    # -------------------------------------------------------------
    # -------------------------------------------------------------
    # Methods to set kernel parameters via API

    # executable
    # pre_exec
    # uses_mpi
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
    def uses_mpi(self):
        return self._uses_mpi

    @uses_mpi.setter
    def uses_mpi(self, uses_mpi):

        if type(uses_mpi) != bool:
            raise TypeError(
                expected_type=bool,
                actual_type=type(uses_mpi))

        # Call the validate_args() method of the plug-in.
        self._uses_mpi = uses_mpi
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
    @property
    def binding_function(self, resource):
        task_desc = self._binding_function(resource)
        self._executable = task_desc['executable']
        self._arguments = task_desc['arguments']
        self._pre_exec = task_desc['pre_exec']

    @binding_function.setter
    def binding_function(self, function):
        self._binding_function = function
    # -------------------------------------------------------------
    @property
    def raw_args(self):
        return self._raw_args

    @raw_args.setter
    def raw_args(self, args):
        self._raw_args = args
    # -------------------------------------------------------------
    
    @property
    def machine_configs(self):
        return self._machine_configs
    
    @machine_configs.setter(self, config):
        self._machine_configs = config
    # -------------------------------------------------------------
    
    @property
    def valid_args(self):
        return self._valid_args
    # -------------------------------------------------------------

    def get_arg(self, arg):
        return self._valid_args[arg]['_value']
    # -------------------------------------------------------------

    def validate_args(self, arguments):

        arg_details = dict()

        try:

            for arg_name, arg_info in self._raw_args.iteritems():
                self._valid_args[arg_name]["_is_set"] = False
                self._valid_args[arg_name]["_value"] = None
                self._valid_args[arg_name]["_is_mandatory"] = arg_info["mandatory"]

            for arg in arguments:
                arg_found = False
                for arg_name, arg_info in self._raw_args.iteritems():
                    if arg.startswith(arg_name):
                        arg_found = True
                        self._valid_args[arg_name]["_is_set"] = True
                        self._valid_args[arg_name]["_value"] = arg.replace(arg_name,'')

                if arg_found == False:
                    raise ArgumentError(
                                        kernel_name=self._kernel_name,
                                        message="Unknown / malformed argument '%s'"%(arg),
                                        valid_arguments_set=self._raw_args)

            for arg_name, arg_info in self._valid_args.iteritems():
                if ((arg_info["_is_mandatory"] == True) and (arg_info["_is_set"] == False)):
                    raise ArgumentError(
                                        kernel_name=self._kernel_name,
                                        message="Mandatory argument '%s' missing"%(arg_name),
                                        valid_arguments_set=self._raw_args)

            #self._args = self._raw_args
            self._logger.debug("Arguments validated for kernel %s"%(self._kernel_name))

        except Exception, ex:
            self._logger.error('Kernel argument validation failed: %s'%(ex))
            raise

    def _bind_to_resource(self, resource):

        try:
            self._binding_function(resource)
        except Exception, ex:
            self._logger.error('Kernel bind to resource failed, error: %s'%ex)
            raise