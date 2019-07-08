
__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__    = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__   = "MIT"

import os

import radical.pilot as rp

from ...                     import exceptions as ree
from ..base.resource_manager import Base_ResourceManager


# ------------------------------------------------------------------------------
#
class ResourceManager(Base_ResourceManager):
    """
    A resource manager takes the responsibility of placing resource requests on
    different, possibly multiple, DCIs. This ResourceManager uses the RADICAL
    Pilot as the underlying runtime system.

    :arguments:
        :resource_desc: dictionary with details of the resource request and
                        access credentials of the user
        :example: resource_desc = {
                                    |  'resource'      : 'xsede.stampede',
                                    |  'walltime'      : 120,
                                    |  'cpus'          : 64,
                                    |  'project'       : 'TG-abcxyz',
                                    |  'queue'         : 'abc',    # optional
                                    |  'access_schema' : 'ssh'  # optional
                                  }
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, resource_desc, sid, rts_config):

        super(ResourceManager, self).__init__(resource_desc=resource_desc,
                                              sid=sid,
                                              rts='radical.pilot',
                                              rts_config=rts_config)
        # RP specific parameters
        self._session             = None
        self._pmgr                = None
        self._pilot               = None
        self._download_rp_profile = False

        if "sandbox_cleanup" not in self._rts_config or \
           "db_cleanup"      not in self._rts_config:

            raise ree.ValueError(obj=self._uid,
                                 attribute='config',
                                 expected_value={"sandbox_cleanup": False,
                                                 "db_cleanup"     : False},
                                 actual_value=self._rts_config)

        self._logger.info('Created resource manager object: %s' % self._uid)
        self._prof.prof('rmgr obj created', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    @property
    def session(self):
        """
        :getter: Return the Radical Pilot session currently being used
        """
        return self._session

    @property
    def pmgr(self):
        """
        :getter: Return the Radical Pilot manager currently being used
        """
        return self._pmgr

    @property
    def pilot(self):
        """
        :getter: Return the submitted Pilot
        """
        return self._pilot


    # --------------------------------------------------------------------------
    #
    def get_resource_allocation_state(self):
        """
        **Purpose**: Get the state of the resource allocation

        """

        if self._pilot:
            return self._pilot.state


    # --------------------------------------------------------------------------
    #
    def get_completed_states(self):
        """
        **Purpose**: return states which signal completed resource allocation

        """

        return rp.FINAL


    # --------------------------------------------------------------------------
    #
    def _submit_resource_request(self):
        """
        **Purpose**: Create and submits a RADICAL Pilot Job as per the user
                     provided resource description
        """

        try:

            self._prof.prof('creating rreq', uid=self._uid)

            # ------------------------------------------------------------------
            def _pilot_state_cb(pilot, state):

                self._logger.info('Pilot %s state: %s' % (pilot.uid, state))

                if state == rp.FAILED:
                    self._logger.error('Pilot has failed')

                elif state == rp.DONE:
                    self._logger.error('Pilot has completed')
            # ------------------------------------------------------------------

            self._session = rp.Session(uid=self._sid)
            self._pmgr    = rp.PilotManager(session=self._session)

            self._pmgr.register_callback(_pilot_state_cb)

            cleanup = self._rts_config.get('sandbox_cleanup')
            pd_init = {'resource'      : self._resource,
                       'runtime'       : self._walltime,
                       'cores'         : self._cpus,
                       'project'       : self._project,
                       'gpus'          : self._gpus,
                       'access_schema' : self._access_schema,
                       'queue'         : self._queue,
                       'cleanup'       : cleanup,
                       }

            # Create Compute Pilot with validated resource description
            pdesc = rp.ComputePilotDescription(pd_init)

            self._prof.prof('rreq created', uid=self._uid)

            # Launch the pilot
            self._pilot = self._pmgr.submit_pilots(pdesc)

            self._prof.prof('rreq submitted', uid=self._uid)

            shared_staging_directives = list()
            for data in self._shared_data:
                temp = {'source': data,
                        'target': 'pilot:///' + os.path.basename(data)
                }
                shared_staging_directives.append(temp)

            self._pilot.stage_in(shared_staging_directives)

            self._prof.prof('shared data staging initiated', uid=self._uid)
            self._logger.info('Resource request submission successful, waiting'
                              'for pilot to become Active')

            # Wait for pilot to go active
            self._pilot.wait([rp.PMGR_ACTIVE, rp.DONE, rp.FAILED, rp.CANCELED])

            self._prof.prof('resource active', uid=self._uid)
            self._logger.info('Pilot is now active')

        except KeyboardInterrupt:

            if self._session:
                self._session.close()

            self._logger.exception('Execution interrupted (probably by Ctrl+C) '
                                   'exit callback thread gracefully...')
            raise KeyboardInterrupt

        except Exception:
            self._logger.exception('Resource request submission failed')
            raise


    # --------------------------------------------------------------------------
    #
    def _terminate_resource_request(self):
        """
        **Purpose**: Cancel the RADICAL Pilot Job
        """

        try:

            if self._pilot:

                self._prof.prof('rreq_cancel', uid=self._uid)
                self._pilot.cancel()

                get_profiles = os.environ.get('RADICAL_PILOT_PROFILE', False)
                cleanup      = self._rts_config.get('db_cleanup', False)

                self._session.close(cleanup=cleanup, download=get_profiles)
                self._prof.prof('rreq_canceled', uid=self._uid)

        except KeyboardInterrupt:

            self._logger.exception('Execution interrupted (probably by Ctrl+C) '
                                   'exit callback thread gracefully...')
            raise KeyboardInterrupt


        except Exception:
            self._logger.exception('Could not cancel resource request')
            raise


# ------------------------------------------------------------------------------

