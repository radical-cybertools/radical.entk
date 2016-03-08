#!/usr/bin/env python

"""This module defines and implements the Kernel class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.engine import Engine
from radical.ensemblemd.exceptions import TypeError


# ------------------------------------------------------------------------------
#
class Kernel(object):

    #---------------------------------------------------------------------------
    #
    def __init__(self, name, args=None,instance_type=None):
        """Create a new Kernel object.
        """
        if type(name) != str:
            raise TypeError(
                expected_type=str,
                actual_type=type(name))

        self._engine = Engine()
        self._kernel = self._engine.get_kernel_plugin(name)
        self._kernel._exists_remote = None

        if args is not None:
            self.set_args(args)

        if (instance_type == 'single'):
            self._kernel.instance_type = 'single'
        else:
            self._kernel.instance_type = 'multiple'
            

    #---------------------------------------------------------------------------
    #
    """
    ANTONS: COMMENTED OUT FOR NOW, HAVE NO IDEA WHAT THIS IS SUPPOSED TO DO!
    Giannis: This is used to conbine to a single list the Kernel's plugin pre
    exec with the commands needed to download any input data the user wants from
    a remote server.
    """
    @property
    def _cu_def_pre_exec(self):

        pre_exec = list()

        # Translate upload directives into cURL command(s)
        if self._kernel._download_input_data is not None:
            for download in self._kernel._download_input_data:

                # see if a rename is requested
                dl = download.split(">")
                if len(dl) == 1:
                    # no rename
                     cmd = "curl --insecure -O {0}".format(dl[0].strip())
                elif len(dl) == 2:
                     cmd = "curl --insecure -L {0} -o {1}".format(dl[0].strip(), dl[1].strip())
                else:
                    # error
                    raise Exception("Invalid transfer directive %s" % download)

                pre_exec.append(cmd)

        # Removed other directives since you are using RP directives in the execution plugin
        
        # Add existing pre-exec.
        if self._kernel._pre_exec is not None:
            pre_exec.extend(self._kernel._pre_exec)

        return pre_exec

    #---------------------------------------------------------------------------
    #
    #@property
    #def _cu_def_pre_exec(self):
    #    return self._kernel._pre_exec

    #---------------------------------------------------------------------------
    #
    @property
    def pre_exec(self):
        return self._kernel._pre_exec

    @pre_exec.setter
    def pre_exec(self, commands):
        self._kernel._pre_exec = commands

    #---------------------------------------------------------------------------
    #
    @property
    def _cu_def_post_exec(self):
        return self._kernel._post_exec

    #---------------------------------------------------------------------------
    #
    @property
    def post_exec(self):
        return self._kernel._post_exec

    @post_exec.setter
    def post_exec(self, commands):
        self._kernel._post_exec = commands

    #---------------------------------------------------------------------------
    #
    @property
    def subname(self):
        return self._kernel._subname

    @subname.setter
    def subname(self, name):
        self._kernel._subname = name

    #---------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._kernel.get_name()

    #---------------------------------------------------------------------------
    #
    @property
    def _cu_def_output_data(self):
        return self._kernel._download_output_data

    #---------------------------------------------------------------------------
    #
    @property
    def _cu_def_input_data(self):
        return self._kernel._upload_input_data

    #---------------------------------------------------------------------------
    #
    @property
    def _cu_def_executable(self):
        return self._kernel._executable

    #---------------------------------------------------------------------------
    #
    @property
    def environment(self):
        return self._kernel._environment

    @environment.setter
    def environment(self,key_vals):

        """Sets the environment for the kernel.
        """
        if type(key_vals) != dict:
            raise TypeError(
                expected_type=dict,
                actual_type=type(key_vals))

        #Iterate through dict and set environment
        for key, val in key_vals.iteritems():
            self._kernel._environment[key] = val

    #---------------------------------------------------------------------------
    #
    @property
    def uses_mpi(self):
        return self._kernel._uses_mpi

    @uses_mpi.setter
    def uses_mpi(self, uses_mpi):

        if type(uses_mpi) != bool:
            raise TypeError(
                expected_type=bool,
                actual_type=type(uses_mpi))

        # Call the validate_args() method of the plug-in.
        self._kernel._uses_mpi = uses_mpi

    #---------------------------------------------------------------------------
    #
    @property
    def arguments(self):
        """List of arguments to the kernel as defined by the kernel definition files"""
        return self._kernel._arguments

    @arguments.setter
    def arguments(self, args):
        """Sets the arguments for the kernel.
        """
        if type(args) != list:
            raise TypeError(
                expected_type=list,
                actual_type=type(args))

        # Call the validate_args() method of the plug-in.
        self._kernel.validate_args(args)

    #---------------------------------------------------------------------------
    #
    @property
    def cores(self):
        """The number of cores the kernel is using.
        """
        return self._kernel._cores

    @cores.setter
    def cores(self, cores):

        if type(cores) != int:
            raise TypeError(
                expected_type=int,
                actual_type=type(cores))

        # Call the validate_args() method of the plug-in.
        self._kernel._cores = cores

    #---------------------------------------------------------------------------
    #
    @property
    def upload_input_data(self):
        """Instructs the application to upload one or more files or directories
           from the host the script is running on into the kernel's
           execution directory.

           Example::

                k = Kernel(name="misc.ccount")
                k.arguments = ["--inputfile=input.txt", "--outputfile=output.txt"]
                k.upload_input_data = ["/location/on/HOST/RUNNING/THE/SCRIPT/data.txt > input.txt"]
        """
        return self._kernel._upload_input_data

    @upload_input_data.setter
    def upload_input_data(self, data_directives):
        if type(data_directives) != list:
            data_directives = [data_directives]

        for dd in data_directives:
            if type(dd) != str:
                raise TypeError(
                    expected_type=str,
                    actual_type=type(dd))

        self._kernel._upload_input_data = data_directives

    #---------------------------------------------------------------------------
    #
    @property
    def download_input_data(self):
        """Instructs the kernel to download one or more files or directories
           from a remote HTTP server into the kernel's execution directory.

           Example::

                k = Kernel(name="misc.ccount")
                k.arguments = ["--inputfile=input.txt", "--outputfile=output.txt"]
                k.download_input_data = ["http://REMOTE.WEBSERVER/location/data.txt > input.txt"]


           .. note:: Supported URL types are ``http://`` and ``https://``.

        """
        return self._kernel._download_input_data

    @download_input_data.setter
    def download_input_data(self, data_directives):

        if type(data_directives) != list:
            data_directives = [data_directives]

        for dd in data_directives:
            if type(dd) != str:
                raise TypeError(
                    expected_type=str,
                    actual_type=type(dd))

        self._kernel._download_input_data = data_directives

    #---------------------------------------------------------------------------
    #
    @property
    def link_input_data(self):
        """Instructs the kernel to create a link to one or more files or
           directories on the execution host's filesystem in the kernel's
           execution directory.

           Example::

                k = Kernel(name="misc.ccount")
                k.arguments = ["--inputfile=input.txt", "--outputfile=output.txt"]
                k.link_input_data = ["/location/on/EXECUTION/HOST/data.txt > input.txt"]
        """
        return self._kernel._link_input_data

    @link_input_data.setter
    def link_input_data(self, data_directives):
        if type(data_directives) != list:
            data_directives = [data_directives]

        for dd in data_directives:
            if type(dd) != str:
                raise TypeError(
                    expected_type=str,
                    actual_type=type(dd))

        self._kernel._link_input_data = data_directives

    #---------------------------------------------------------------------------
    #
    @property
    def download_output_data(self):
        """Instructs the application to download one or more files or directories
           from the kernel's execution directory back to the
           host the script is running on.

           Example::

                k = Kernel(name="misc.ccount")
                k.arguments = ["--inputfile=input.txt", "--outputfile=output.txt"]
                k.download_output_data = ["output.txt > output-run-1.txt"]
        """
        return self._kernel._download_output_data

    @download_output_data.setter
    def download_output_data(self, data_directives):

        if type(data_directives) != list:
            data_directives = [data_directives]

        for dd in data_directives:
            if type(dd) != str:
                raise TypeError(
                    expected_type=str,
                    actual_type=type(dd))

        self._kernel._download_output_data = data_directives

    #---------------------------------------------------------------------------
    #
    @property
    def copy_input_data(self):
        """Instructs the kernel to copy one or more files or directories from
           the execution host's filesystem into the kernel's execution
           directory.

           Example::

                k = Kernel(name="misc.ccount")
                k.arguments = ["--inputfile=input.txt", "--outputfile=output.txt"]
                k.copy_input_data = ["/location/on/EXECUTION/HOST/data.txt > input.txt"]

        """
        return self._kernel._copy_input_data

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

        self._kernel._copy_input_data = data_directives

    #---------------------------------------------------------------------------
    #
    @property
    def copy_output_data(self):
        """Instructs the application to copy one or more files or directories
           from the kernel's execution directory to a directory on the
           execution host.

           Example::

                k = Kernel(name="misc.ccount")
                k.arguments = ["--inputfile=input.txt", "--outputfile=output.txt"]
                k.copy_output_data = ["output.txt > /home/me/results/result1.txt"]
        """
        return self._kernel._copy_output_data

    @copy_output_data.setter
    def copy_output_data(self, data_directives):

        if type(data_directives) != list:
            data_directives = [data_directives]

        for dd in data_directives:
            if type(dd) != str:
                raise TypeError(
                    expected_type=str,
                    actual_type=type(dd))

        self._kernel._copy_output_data = data_directives

    #---------------------------------------------------------------------------
    #
    def get_raw_args(self):
        """Returns the arguments  passed to the kernel.
        """
        return self._kernel.get_arg(name)

    #---------------------------------------------------------------------------
    #
    def get_arg(self, name):
        """Returns the value of the kernel argument given by 'arg_name'.
        """
        return self._kernel.get_arg(name)


    #---------------------------------------------------------------------------
    #
    @property
    def get_instance_type(self):
        """Returns the instance_type of the kernel. It can be 'single' or
        'multiple'
        """
        return self._kernel.instance_type


    #---------------------------------------------------------------------------
    #
    @property
    def exists_remote(self):
        """Method to check if a kernel output file (or a list of files) exists on remote. This method is useful when 
        using wrappers around executables
        """
        return self._kernel._exists_remote

    @exists_remote.setter
    def exists_remote(self, files_list):

        if type(files_list) != list:
            files_list = [files_list]

        self._kernel._exists_remote = files_list
    

    #---------------------------------------------------------------------------
    #
    def _bind_to_resource(self, resource_key, pattern_name=None):
        """Returns the kernel description as a dictionary that can be
           translated into a CU description.
        """
        if (pattern_name == None):
            self._kernel._bind_to_resource(resource_key)
        else:
            self._kernel._bind_to_resource(resource_key, pattern_name)
        return self._kernel
