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
        :example: resource_desc = { 'resource'      : 'xsede.stampede', 
                                    'walltime'      : 120, 
                                    'cores'         : 64, 
                                    'project'       : 'TG-abcxyz',
                                    'queue'         : 'abc',    # optional
                                    'access_schema' : 'ssh'  # optional
                                }
    """

    def __init__(self, resource_desc, sid):      

        super(ResourceManager, self).__init__(  resource_desc=resource_desc,
                                                sid=sid,
                                                rts='radical.pilot')


        # RP specific parameters
        self._session = None
        self._pmgr = None
        self._pilot = None
        self._download_rp_profile = False

        self._mlab_url = os.environ.get('RADICAL_PILOT_DBURL', None)
        if not self._mlab_url:
            raise Error(text='RADICAL_PILOT_DBURL not defined. Please assign a valid mlab url')

        self._logger.info('Created resource manager object: %s' % self._uid)
        self._prof.prof('rmgr obj created', uid=self._uid)
    # ------------------------------------------------------------------------------------------------------------------
    # Getter methods
    # ------------------------------------------------------------------------------------------------------------------

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
    def pilot(self):
        """
        :getter: Return reference to the submitted Pilot
        """
        return self._pilot

    @property
    def sid(self):
        return self._sid

    @sid.setter
    def sid(self, val):
        self._sid = sid
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
        **Purpose**: Test if a resource allocation was submitted

        """

        return [rp.CANCELED, rp.FAILED, rp.DONE]

    # ------------------------------------------------------------------------------------------------------------------
    # Private methods
    # ------------------------------------------------------------------------------------------------------------------

    def _validate_resource_desc(self):
        """
        **Purpose**: Validate the resource description that was provided to ResourceManager

        :arguments: 
            :sid: session id of the current run
        :return: boolean (valid/invalid)
        """

        self._uid = ru.generate_id('resource_manager.%(item_counter)04d', ru.ID_CUSTOM, namespace=self._sid)
        self._path = os.getcwd() + '/' + self._sid
        self._logger = ru.Logger('radical.entk.%s' % self._uid, path=self._path)
        self._prof = ru.Profiler(name='radical.entk.%s' % self._uid, path=self._path)

        try:

            self._prof.prof('validating rdesc', uid=self._uid)

            self._logger.debug('Validating resource description')

            expected_keys = ['resource',
                             'walltime',
                             'cores']

            for key in expected_keys:
                if key not in self._resource_desc:
                    raise Error(text='Mandatory key %s does not exist in the resource description' % key)

            if not isinstance(self._resource_desc['resource'], str):
                raise TypeError(expected_type=str, actual_type=type(self._resource_desc['resource']))

            if not isinstance(self._resource_desc['walltime'], int):
                raise TypeError(expected_type=int, actual_type=type(self._resource_desc['walltime']))

            if not isinstance(self._resource_desc['cores'], int):
                raise TypeError(expected_type=int, actual_type=type(self._resource_desc['cores']))

            if 'project' in self._resource_desc:
                if (not isinstance(self._resource_desc['project'], str)) and (not self._resource_desc['project']):
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
        **Purpose**: Populate the RP_ResourceManager attributes with values provided in the resource description
        """

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

    def _submit_resource_request(self):
        """
        **Purpose**: Create and submits a RADICAL Pilot Job as per the user 
                     provided resource description
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

            self._prof.prof('rreq submitted', uid=self._uid)

            shared_staging_directives = list()
            for data in self._shared_data:
                temp = {
                    'source': data,
                    'target': 'pilot:///' + os.path.basename(data)
                }
                shared_staging_directives.append(temp)

            self._pilot.stage_in(shared_staging_directives)

            self._prof.prof('shared data staging initiated', uid=self._uid)
            self._logger.info('Resource request submission successful.. waiting for pilot to go Active')

            # Wait for pilot to go active
            self._pilot.wait([rp.PMGR_ACTIVE, rp.FAILED])

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

    def _cancel_resource_request(self):
        """
        **Purpose**: Cancel the RADICAL Pilot Job
        """

        try:

            if self._pilot:

                self._prof.prof('canceling resource allocation', uid=self._uid)
                self._pilot.cancel()
                download_rp_profile = os.environ.get('RADICAL_PILOT_PROFILE', False)
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
