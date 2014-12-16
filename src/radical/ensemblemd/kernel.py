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
    def __init__(self, name, args=None):
        """Create a new Kernel object.
        """
        if type(name) != str:
            raise TypeError(
                expected_type=str,
                actual_type=type(name))

        self._engine = Engine()
        self._kernel = self._engine.get_kernel_plugin(name)

        if args is not None:
            self.set_args(args)

    #---------------------------------------------------------------------------
    #
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

        # Translate copy directives into cp command(s)
        if self._kernel._copy_input_data is not None:
            for copy in self._kernel._copy_input_data:

                # see if a rename is requested
                dl = copy.split(">")
                if len(dl) == 1:
                    # no rename
                     cmd = "cp -r {0} .".format(dl[0].strip())
                elif len(dl) == 2:
                     cmd = "cp -r {0} ./{1}".format(dl[0].strip(), dl[1].strip())
                else:
                    # error
                    raise Exception("Invalid transfer directive %s" % copy)

                pre_exec.append(cmd)

        # Translate link directives into ln command(s)
        if self._kernel._link_input_data is not None:
            for link in self._kernel._link_input_data:

                # see if a rename is requested
                dl = link.split(">")
                if len(dl) == 1:
                    # no rename
                     cmd = "ln -s {0}".format(dl[0].strip())
                elif len(dl) == 2:
                     cmd = "ln -s {0} {1}".format(dl[0].strip(), dl[1].strip())
                else:
                    # error
                    raise Exception("Invalid transfer directive %s" % link)

                pre_exec.append(cmd)

        # Add existing pre-exec.
        if self._kernel._pre_exec is not None:
            pre_exec.extend(self._kernel._pre_exec)

        return pre_exec

    #---------------------------------------------------------------------------
    #
    @property
    def _cu_def_post_exec(self):
        return self._kernel._post_exec

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

    #---------------------------------------------------------------------------
    #
    @property
    def uses_mpi(self):
        return self._kernel._uses_mpi

    #---------------------------------------------------------------------------
    #
    @property
    def cores(self):
        return self._kernel._cores

    #---------------------------------------------------------------------------
    #
    @property
    def arguments(self):
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
                actual_type=type(args))

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
            if type(dd) != str:
                raise TypeError(
                    expected_type=str,
                    actual_type=type(dd))

        self._kernel._copy_input_data = data_directives

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
    def _bind_to_resource(self, resource_key):
        """Returns the kernel description as a dictionary that can be
           translated into a CU description.
        """
        self._kernel._bind_to_resource(resource_key)
        return self._kernel
