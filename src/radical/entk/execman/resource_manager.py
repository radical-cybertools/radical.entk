__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__ = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__ = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import radical.pilot as rp
import os


class ResourceManager(object):

    """
    A resource manager takes the responsibility of placing resource requests on different, possibly multiple,
    DCIs. Currently, the runtime system being used is RADICAL Pilot and hence the resource request is made via
    Pilot Jobs.

    :arguments: 
        :resource_desc: dictionary with details of the resource request + access credentials of the user 
        :example: resource_desc = {  'resource': 'xsede.stampede', 'walltime': 120, 'cores': 64, 'project: 'TG-abcxyz'}

    """

    def __init__(self, resource_desc):

        self._resource_desc = resource_desc

        self._session = None
        self._pmgr = None
        self._pilot = None
        self._resource = None
        self._walltime = None
        self._cores = None
        self._project = None
        self._access_schema = None
        self._queue = None

        self._mlab_url = os.environ.get('RADICAL_PILOT_DBURL', None)
        if not self._mlab_url:
            raise Error(text='RADICAL_PILOT_DBURL not defined. Please assign a valid mlab url')

        # Shared data list
        self._shared_data = list()

    # ------------------------------------------------------------------------------------------------------------------
    # Getter methods
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def pilot(self):
        """
        :getter: Return reference to the submitted Pilot
        """
        return self._pilot

    @property
    def session(self):
        """
        :getter: Return reference to the Radical Pilot session instance currently being used
        """
        return self._session

    @property
    def pmgr(self):
        """
        :getter: Return reference to the Radical Pilot manager currently being used
        """
        return self._pmgr

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
    def cores(self):
        """
        :getter: Return user specified number of cpus
        """
        return self._cores

    @property
    def project(self):
        """
        :getter: Return user specified project ID
        """
        return self._project

    @property
    def access_schema(self):
        """
        :getter: Return user specified access schema -- 'ssh' or 'gsissh' or None
        """
        return self._access_schema

    @property
    def queue(self):
        """
        :getter: Return user specified resource queue to be used
        """
        return self._queue

    @property
    def shared_data(self):
        """
        :getter: list of files to be staged to remote and that are common to multiple tasks
        """
        return self._shared_data

    @shared_data.setter
    def shared_data(self, data_list):

        if not isinstance(data_list, list):
            data_list = [data_list]

        for val in data_list:
            if not isinstance(val, str):
                raise TypeError(expected_type=str, actual_type=type(val))

        self._shared_data = data_list

    # ------------------------------------------------------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------------------------------------------------------

    def get_resource_allocation_state(self):
        """
        **Purpose**: Get the state of the resource allocation

        """

        if self._pilot:
            return self._pilot.state
        else:
            return None

    def completed_states(self):
        """
        **Purpose**: Test if a resouce allocation was submitted

        """

        return [rp.CANCELED, rp.FAILED, rp.DONE]

    # ------------------------------------------------------------------------------------------------------------------
    # Private methods
    # ------------------------------------------------------------------------------------------------------------------

    def _validate_resource_desc(self, sid):
        """
        **Purpose**: Validate the resource description that was provided to ResourceManager

        :arguments: dictionary consisting of details of the resource request
        :return: boolean (valid/invalid)
        """

        self._uid = ru.generate_id('radical.entk.resource_manager.%(item_counter)04d', ru.ID_CUSTOM, namespace=sid)
        self._path = os.getcwd() + '/' + sid

        self._logger = ru.get_logger(self._uid, path=self._path)
        self._prof = ru.Profiler(name=self._uid, path=self._path)

        try:
            
            self._prof.prof('validating rdesc', uid=self._uid)

            self._logger.debug('Validating resource description')

            if not isinstance(self._resource_desc, dict):
                raise TypeError(expected_type=dict, actual_type=type(self._resource_desc))

            expected_keys = [   'resource',
                                'walltime',
                                'cores'
                            ]

            for key in expected_keys:
                if key not in self._resource_desc:
                    raise Error(text='Key %s does not exist in the resource description' % key)

            if not isinstance(self._resource_desc['resource'], str):
                raise TypeError(expected_type=str, actual_type=type(self._resource_desc['resource']))

            if not isinstance(self._resource_desc['walltime'], int):
                raise TypeError(expected_type=int, actual_type=type(self._resource_desc['walltime']))

            if not isinstance(self._resource_desc['cores'], int):
                raise TypeError(expected_type=int, actual_type=type(self._resource_desc['cores']))

            if 'project' in self._resource_desc:
                if (not isinstance(self._resource_desc['project'],str)) and (not self._resource_desc['project']):
                    raise TypeError(expected_type=str, actual_type=type(self._resource_desc['project']))            

            if 'access_schema' in self._resource_desc:
                if not isinstance(self._resource_desc['access_schema'], str):
                    raise TypeError(expected_type=str, actual_type=type(self._resource_desc['access_schema']))

            if 'queue' in self._resource_desc:
                if not isinstance(self._resource_desc['queue'], str):
                    raise TypeError(expected_type=str, actual_type=type(self._resource_desc['queue']))

            self._logger.info('Resource description validated')

            self._prof.prof('rdesc validated', uid=self._uid)

            return True

        except Exception, ex:
            self._logger.error('Failed to validate resource description, error: %s' % ex)
            raise

    def _populate(self):
        """
        **Purpose**: Populate the ResourceManager attributes with values provided in the resource description

        :arguments: valid dictionary consisting of details of the resource request
        """

        try:

            self._prof.prof('populating rmgr', uid=self._uid)

            self._logger.debug('Populating resource manager object')

            self._resource = self._resource_desc['resource']
            self._walltime = self._resource_desc['walltime']
            self._cores = self._resource_desc['cores']

            if 'project' in self._resource_desc:
                self._project = self._resource_desc['project']

            if 'access_schema' in self._resource_desc:
                self._access_schema = self._resource_desc['access_schema']

            if 'queue' in self._resource_desc:
                self._queue = self._resource_desc['queue']

            self._logger.debug('Resource manager population successful')

            self._prof.prof('rmgr populated', uid=self._uid)

        except Exception, ex:
            self._logger.error('Resource manager population unsuccessful')
            raise

    def _submit_resource_request(self):
        """
        **Purpose**: Function to initiate the resource request.

        Details: Currently, submits a Pilot job using the RADICAL Pilot runtime system.
        """

        try:

            self._prof.prof('creating rreq', uid=self._uid)

            def _pilot_state_cb(pilot, state):
                self._logger.info('Pilot %s state: %s' % (pilot.uid, state))

                if state == rp.FAILED:
                    self._logger.error('Pilot has failed')

                elif state == rp.DONE:
                    self._logger.error('Pilot has completed')

            self._session = rp.Session(dburl=self._mlab_url)

            self._pmgr = rp.PilotManager(session=self._session)
            self._pmgr.register_callback(_pilot_state_cb)

            pd_init = {
                'resource': self._resource,
                'runtime': self._walltime,
                'cores': self._cores,
                'project': self._project,
            }

            if self._access_schema:
                pd_init['access_schema'] = self._access_schema

            if self._queue:
                pd_init['queue'] = self._queue

            # Create Compute Pilot with validated resource description
            pdesc = rp.ComputePilotDescription(pd_init)

            self._prof.prof('rreq created', uid=self._uid)

            # Launch the pilot
            self._pilot = self._pmgr.submit_pilots(pdesc)

            shared_staging_directives = list()
            for data in self._shared_data:
                temp = {
                    'source': data,
                    'target': 'pilot:///' + os.path.basename(data)
                }
                shared_staging_directives.append(temp)

            self._pilot.stage_in(shared_staging_directives)

            self._prof.prof('rreq submitted', uid=self._uid)

            self._logger.info('Resource request submission successful.. waiting for pilot to go Active')

            # Wait for pilot to go active
            self._pilot.wait([rp.PMGR_ACTIVE, rp.FAILED])

            if self._pilot.state == rp.FAILED:
                raise Exception

            self._prof.prof('resource active', uid=self._uid)

            self._logger.info('Pilot is now active')

        except KeyboardInterrupt:

            if self._session:
                self._session.close()

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                               'trying to exit callback thread gracefully...')
            raise KeyboardInterrupt

        except Exception, ex:
            self._logger.error('Resource request submission failed')
            raise

    def _cancel_resource_request(self, download_rp_profile=False):

        """
        **Purpose**: Cancel the resource request
        """

        try:

            self._prof.prof('canceling resource allocation', uid=self._uid)
            self._pilot.cancel()
            self._session.close(cleanup=False, download=download_rp_profile)
            self._prof.prof('resource allocation cancelled', uid=self._uid)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                               'trying to exit callback thread gracefully...')
            raise KeyboardInterrupt

        except Exception, ex:
            self._logger.error('Could not cancel resource request, error: %s' % ex)
            raise

    # ------------------------------------------------------------------------------------------------------------------
