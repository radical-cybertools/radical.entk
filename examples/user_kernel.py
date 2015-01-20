#!/usr/bin/env python

"""
TODO

Run Locally
^^^^^^^^^^^

.. warning:: In order to run this example, you need access to a MongoDB server and
             set the ``RADICAL_PILOT_DBURL`` in your environment accordingly.
             The format is ``mongodb://hostname:port``. Read more about it
             MongoDB in chapter :ref:`envpreparation`.

**Step 1:** View and download the example sources :ref:`below <example_source_bag_of_ensembles>`.

**Step 2:** Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about simulation progress::

    RADICAL_ENMD_VERBOSE=info python user_kernels.py

Run on a Remote Cluster
^^^^^^^^^^^^^^^^^^^^^^^

By default, this Bag of Ensembles runs on one core your local machine::

    SingleClusterEnvironment(
        resource="localhost",
        cores=1,
        walltime=30,
        username=None,
        allocation=None
    )

You can change the script to use a remote HPC cluster and increase the number
of cores to see how this affects the runtime of the script as the individual
ensembles in the bag can run in parallel::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu",
        cores=16,
        walltime=30,
        username=None,  # add your username here
        allocation=None # add your allocation or project id here if required
    )

.. _example_source_user_kernels:

Example Source
^^^^^^^^^^^^^^
"""

__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "User-Defined Application Kernel Example "


from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

from radical.ensemblemd.engine import get_engine
from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase


# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {
    "name":         "my.user.defined.kernel",
    "description":  "Brief description of your Kernel.",
    "arguments":   {
        "--sleep-interval=": {
            "mandatory": True,
            "description": "Number of seconds to do nothing."
        },
    }
}

# ------------------------------------------------------------------------------
#
class MyUserDefinedKernel(KernelBase):

    def __init__(self):
        """Le constructor.
        """
        super(MyUserDefinedKernel, self).__init__(_KERNEL_INFO)

    @staticmethod
    def get_name():
        return _KERNEL_INFO["name"]

    def _bind_to_resource(self, resource_key):
        """This function binds the Kernel to a specific resource defined in
           "resource_key".
        """
        executable = "/bin/sleep"
        arguments  = ['{0}'.format(self.get_arg("--sleep-interval="))]

        self._executable  = executable
        self._arguments   = arguments
        self._environment = None
        self._uses_mpi    = False
        self._pre_exec    = None
        self._post_exec   = None

# ------------------------------------------------------------------------------
# Register the user-defined kernel with Ensemble MD.
get_engine().add_kernel_plugin(MyUserDefinedKernel)

# ------------------------------------------------------------------------------
#
class Sleep(Pipeline):
    """The Sleep class implements a Bag of Task. All ensemble members
       simply sleep for 60 seconds.
    """

    def __init__(self, instances):
        Pipeline.__init__(self, instances)

    def step_1(self, instance):
        """This step sleeps for 60 seconds.
        """
        k = Kernel(name="my.user.defined.kernel")
        k.arguments = ["--sleep-interval=60"]
        return k

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
            resource="localhost",
            cores=1,
            walltime=15,
            username=None,
            allocation=None
        )

        # Allocate the resources.
        cluster.allocate()

        # Set the 'instances' of the pipeline to 16. This means that 16 instances
        # of each pipeline step are executed.
        #
        # Execution of the 16 pipeline instances can happen concurrently or
        # sequentially, depending on the resources (cores) available in the
        # SingleClusterEnvironment.
        sleep = Sleep(instances=16)

        cluster.run(sleep)

    except EnsemblemdError, er:

        print "Ensemble MD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
