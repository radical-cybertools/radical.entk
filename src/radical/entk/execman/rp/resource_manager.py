__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__ = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__ = "MIT"

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

    def __init__(self, resource_desc, sid, rts_config):

        super(ResourceManager, self).__init__(resource_desc=resource_desc,
                                              sid=sid,
                                              rts='radical.pilot',
                                              rts_config=rts_config)

        # RP specific parameters
        self._session = None
        self._pmgr = None
        self._pilot = None
        self._download_rp_profile = False

        self._mlab_url = os.environ.get('RADICAL_PILOT_DBURL', None)
        if not self._mlab_url:
            raise EnTKError(msg='RADICAL_PILOT_DBURL not defined. Please assign a valid mlab url')

        if (not "sandbox_cleanup" in self._rts_config.keys()) or (not "db_cleanup" in self._rts_config.keys()):
            raise ValueError(obj=self._uid,
                             attribute='config',
                             expected_value={"sandbox_cleanup": False, "db_cleanup": False},
                             actual_value=self._rts_config)

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

    def get_completed_states(self):
        """
        **Purpose**: Test if a resource allocation was submitted

        """

        return [rp.CANCELED, rp.FAILED, rp.DONE]

    # ------------------------------------------------------------------------------------------------------------------
    # Private methods
    # ------------------------------------------------------------------------------------------------------------------

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

            self._session = rp.Session(dburl=self._mlab_url, uid=self._sid)
            self._pmgr = rp.PilotManager(session=self._session)
            self._pmgr.register_callback(_pilot_state_cb)

            pd_init = {
                'resource': self._resource,
                'runtime': self._walltime,
                'cores': self._cpus,
                'project': self._project,
            }

            if self._gpus:
                pd_init['gpus'] = self._gpus

            if self._access_schema:
                pd_init['access_schema'] = self._access_schema

            if self._queue:
                pd_init['queue'] = self._queue

            if self._rts_config.get('sandbox_cleanup', None):
                pd_init['cleanup'] = True

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

    def _terminate_resource_request(self):
        """
        **Purpose**: Cancel the RADICAL Pilot Job
        """

        try:

            if self._pilot:

                self._prof.prof('canceling resource allocation', uid=self._uid)
                self._pilot.cancel()
                download_rp_profile = os.environ.get('RADICAL_PILOT_PROFILE', False)
                self._session.close(cleanup=self._rts_config.get('db_cleanup', False),
                                    download=download_rp_profile)
                self._prof.prof('resource allocation canceled', uid=self._uid)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                               'trying to exit callback thread gracefully...')
            raise KeyboardInterrupt

        except Exception, ex:
            self._logger.error('Could not cancel resource request, error: %s' % ex)
            raise

    # ------------------------------------------------------------------------------------------------------------------
