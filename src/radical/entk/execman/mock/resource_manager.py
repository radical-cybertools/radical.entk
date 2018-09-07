__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__ = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__ = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import radical.pilot as rp
import os
from ..base.resource_manager import Base_ResourceManager


class ResourceManager(Base_ResourceManager):

    """
    A resource manager takes the responsibility of placing resource requests on
    different, possibly multiple, DCIs. This ResourceManager uses the RADICAL
    Pilot as the underlying runtime system.

    :arguments:
        :resource_desc: dictionary with details of the resource request + access credentials of the user
        :example: resource_desc = {
                                    |  'resource'      : 'xsede.stampede',
                                    |  'walltime'      : 120,
                                    |  'cpus'         : 64,
                                    |  'project'       : 'TG-abcxyz',
                                    |  'queue'         : 'abc',    # optional
                                    |  'access_schema' : 'ssh'  # optional
                                }
    """

    def __init__(self, resource_desc, sid):

        super(ResourceManager, self).__init__(resource_desc=resource_desc,
                                              sid=sid,
                                              rts='mock',
                                              rts_config={})

    # ------------------------------------------------------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------------------------------------------------------

    def get_resource_allocation_state(self):
        """
        **Purpose**: Get the state of the resource allocation

        """
        return None

    def get_completed_states(self):
        """
        **Purpose**: Test if a resource allocation was submitted

        """
        return []

    # ------------------------------------------------------------------------------------------------------------------
    # Private methods
    # ------------------------------------------------------------------------------------------------------------------

    def _validate_resource_desc(self):
        """
        **Purpose**: Validate the resource description that was provided to ResourceManager
        """

        return True

    def _populate(self):
        """
        **Purpose**: Populate the RP_ResourceManager attributes with values provided in the resource description
        """

        return None

    def _submit_resource_request(self):
        """
        **Purpose**: Create and submits a RADICAL Pilot Job as per the user
                     provided resource description
        """

        return None

    def _terminate_resource_request(self):
        """
        **Purpose**: Cancel the RADICAL Pilot Job
        """

        return None
    # ------------------------------------------------------------------------------------------------------------------
