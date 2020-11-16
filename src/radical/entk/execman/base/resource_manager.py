
__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__    = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__   = "MIT"

import os

import radical.utils as ru

from ...exceptions import MissingError, TypeError, EnTKError


# ------------------------------------------------------------------------------
#
class Base_ResourceManager(object):
    """
    A resource manager takes the responsibility of placing resource requests on
    different, possibly multiple, DCIs.

    :arguments:
        :resource_desc: dictionary with details of the resource request
                        and access credentials of the user
        :example: resource_desc = {
                                    |  'resource'      : 'xsede.stampede',
                                    |  'walltime'      : 120,
                                    |  'cpus'          : 64,
                                    |  'gpus'          : 0,          # optional
                                    |  'project'       : 'TG-abcxyz',
                                    |  'queue'         : 'abc',      # optional
                                    |  'access_schema' : 'ssh'       # optional
                                    |  'job_name'      : 'test_job'  # optional}
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, resource_desc, sid, rts, rts_config):

        if not isinstance(resource_desc, dict):
            raise TypeError(expected_type=dict, actual_type=type(resource_desc))

        self._resource_desc = resource_desc
        self._sid           = sid
        self._rts           = rts
        self._rts_config    = rts_config

        # Resource reservation related parameters
        self._resource      = None
        self._walltime      = None
        self._cpus          = 1
        self._gpus          = 0
        self._project       = None
        self._access_schema = None
        self._queue         = None
        self._job_name      = None
        self._validated     = False

        # Utility parameters
        self._uid = ru.generate_id('resource_manager.%(counter)04d',
                                   ru.ID_CUSTOM)
        self._path = os.getcwd() + '/' + self._sid

        name = 'radical.entk.%s' % self._uid
        self._logger = ru.Logger  (name, path=self._path)
        self._prof   = ru.Profiler(name, path=self._path)

        self._shared_data = list()
        self._outputs     = None


    # --------------------------------------------------------------------------
    #
    @property
    def resource(self):
        """
        :getter: Return user specified resource name
        """
        return self._resource


    @property
    def walltime(self):
        """
        :getter: Return user specified walltime
        """
        return self._walltime


    @property
    def cpus(self):
        """
        :getter: Return user specified number of cpus
        """
        return self._cpus


    @property
    def gpus(self):
        """
        :getter: Return user specified number of gpus
        """
        return self._gpus


    @property
    def project(self):
        """
        :getter: Return user specified project ID
        """
        return self._project


    @property
    def access_schema(self):
        """
        :getter:    Return user specified access schema -- 'ssh' or 'gsissh' or
                    None
        """
        return self._access_schema


    @property
    def queue(self):
        """
        :getter: Return user specified resource queue to be used
        """
        return self._queue


    @property
    def job_name(self):
        """
        :getter:    Return user specified job_name
        """
        return self._job_name


    @property
    def shared_data(self):
        """
        :getter:    list of files to be staged to remote and that are common to
                    multiple tasks
        :setter:    Assign a list of names of files that need to be accessible to
                    tasks
        """
        return self._shared_data


    @property
    def outputs(self):
        """
        :getter:    list of files to be staged from remote after execution
        :setter:    Assign a list of names of files that need to be staged from the
                    remote machine
        """
        return self._outputs


    # --------------------------------------------------------------------------
    # Setter functions
    #
    @shared_data.setter
    def shared_data(self, data_list):

        self._shared_data = data_list


    @outputs.setter
    def outputs(self, data):

        self._outputs = data


    # --------------------------------------------------------------------------
    #
    def get_resource_allocation_state(self):
        """
        **Purpose**: Get the state of the resource allocation
        """
        raise NotImplementedError('get_resource_allocation_state() method not '
                                  'implemented in ResourceManager for %s'
                                  % self._rts)


    # --------------------------------------------------------------------------
    #
    def get_completed_states(self):
        """
        **Purpose**: Test if a resource allocation was submitted
        """

        raise NotImplementedError('completed_states() method not implemented '
                                  'in ResourceManager for %s' % self._rts)


    # --------------------------------------------------------------------------
    #
    def _validate_resource_desc(self):
        """
        **Purpose**: Validate the provided resource description
        """

        self._prof.prof('rdesc_validate', uid=self._uid)
        self._logger.debug('Validating resource description')

        expected_keys = ['resource',
                         'walltime',
                         'cpus']

        for key in expected_keys:
            if key not in self._resource_desc:
                raise MissingError(obj='resource description',
                                   missing_attribute=key)

        if not isinstance(self._resource_desc['resource'], str):
            raise TypeError(expected_type=str,
                            actual_type=type(self._resource_desc['resource']))

        if not isinstance(self._resource_desc['walltime'], int):
            raise TypeError(expected_type=int,
                            actual_type=type(self._resource_desc['walltime']))

        if not isinstance(self._resource_desc['cpus'], int):
            raise TypeError(expected_type=int,
                            actual_type=type(self._resource_desc['cpus']))

        if 'gpus' in self._resource_desc:
            if not isinstance(self._resource_desc['gpus'], int):
                raise TypeError(expected_type=int,
                               actual_type=type(self._resource_desc['gpus']))

        if 'project' in self._resource_desc:
            if  not isinstance(self._resource_desc['project'], str):
                raise TypeError(expected_type=str,
                              actual_type=type(self._resource_desc['project']))

        if 'access_schema' in self._resource_desc:
            if not isinstance(self._resource_desc['access_schema'], str):
                raise TypeError(expected_type=str,
                         actual_type=type(self._resource_desc['access_schema']))

        if 'queue' in self._resource_desc:
            if not isinstance(self._resource_desc['queue'], str):
                raise TypeError(expected_type=str,
                                actual_type=type(self._resource_desc['queue']))

        if not isinstance(self._rts_config, dict):
            raise TypeError(expected_type=dict,
                            actual_type=type(self._rts_config))

        self._logger.info('Resource description validated')
        self._prof.prof('rdesc_valid', uid=self._uid)

        self._validated = True

        return self._validated


    # --------------------------------------------------------------------------
    #
    def _populate(self):
        """
        **Purpose**:    Populate the ResourceManager class with the validated
                        resource description
        """

        if not self._validated:
            raise EnTKError('Resource description not validated')


        self._prof.prof('populating rmgr', uid=self._uid)
        self._logger.debug('Populating resource manager object')

        self._resource      = self._resource_desc['resource']
        self._walltime      = self._resource_desc['walltime']
        self._cpus          = self._resource_desc['cpus']
        self._gpus          = self._resource_desc.get('gpus',          0)
        self._project       = self._resource_desc.get('project',       None)
        self._access_schema = self._resource_desc.get('access_schema', None)
        self._queue         = self._resource_desc.get('queue',         None)
        self._job_name      = self._resource_desc.get('job_name',      None)

        self._logger.debug('Resource manager population successful')
        self._prof.prof('rmgr populated', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _submit_resource_request(self):
        """
        **Purpose**:  Submit resource request per provided description
        """

        raise NotImplementedError('_submit_resource_request() method not '
                            'implemented in ResourceManager for %s' % self._rts)


    # --------------------------------------------------------------------------
    #
    def _terminate_resource_request(self):
        """
        **Purpose**:  Cancel resource request by terminating any reservation
                      on any acquired resources or resources pending acquisition
        """

        raise NotImplementedError('_terminate_resource_request() method not '
                            'implemented in ResourceManager for %s' % self._rts)


# ------------------------------------------------------------------------------

