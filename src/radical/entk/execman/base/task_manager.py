
__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__license__   = "MIT"


import os

import radical.utils as ru

from ...exceptions import EnTKError, EnTKTypeError

from .resource_manager import Base_ResourceManager


# ------------------------------------------------------------------------------
#
class Base_TaskManager(object):
    """
    A Task Manager takes the responsibility of dispatching tasks it receives
    from a 'pending' queue for execution on to the available resources using a
    runtime system. Once the tasks have completed execution, they are pushed
    on to the completed queue for other components of EnTK to thread.

    :arguments:
        :rmgr:              (ResourceManager) Object to be used to access the
                            Pilot where the tasks can be submitted

    Currently, EnTK is configured to work with one pending queue and one
    completed queue. In the future, the number of queues can be varied for
    different throughput requirements at the cost of additional Memory and CPU
    consumption.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, sid, rmgr, rts, zmq_info):

        if not isinstance(sid, str):
            raise EnTKTypeError(expected_type=str,
                            actual_type=type(sid))

        if not isinstance(rmgr, Base_ResourceManager):
            raise EnTKTypeError(expected_type=Base_ResourceManager,
                            actual_type=type(rmgr))

        if not isinstance(zmq_info, dict):
            raise EnTKTypeError(expected_type=dict,
                            actual_type=type(zmq_info))

        self._sid  = sid
        self._rmgr = rmgr
        self._rts  = rts

        # Utility parameters
        self._uid  = ru.generate_id('task_manager.%(counter)04d', ru.ID_CUSTOM)
        self._path = os.getcwd() + '/' + self._sid

        name = 'radical.entk.%s' % self._uid
        self._log  = ru.Logger  (name, path=self._path)
        self._prof = ru.Profiler(name, path=self._path)
        self._dh = ru.DebugHelper(name=name)

        self._tmgr_thread = None
        self._tmgr_terminate = None

        self._zmq_queue = None
        self._zmq_info  = zmq_info
        self._setup_zmq(zmq_info)


    # --------------------------------------------------------------------------
    #
    def _setup_zmq(self, zmq_info):

        self._prof.prof('zmq_setup_start', uid=self._uid)

        sid = self._sid
        self._zmq_queue = {
                'put' : ru.zmq.Putter(sid, url=zmq_info['put'], path=sid),
                'get' : ru.zmq.Getter(sid, url=zmq_info['get'], path=sid)}

        self._prof.prof('zmq_setup_stop', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _tmgr(self, uid, rmgr, zmq_info):
        """
        **Purpose**: Method to be run by the tmgr thread. This method receives
                     a Task from the 'pending' queue and submits it to the RTS.
                     At all state transititons, they are synced (blocking) with
                     the AppManager in the master thread.

        **Details**: The AppManager can re-invoke the tmgr thread with this
                     function if the execution of the workflow is still
                     incomplete. There is also population of a dictionary,
                     placeholder_dict, which stores the path of each of the
                     tasks on the remote machine.
        """

        raise NotImplementedError('_tmgr() method not implemented in '
                                  'TaskManager for %s' % self._rts)


    # --------------------------------------------------------------------------
    #
    def _sync_with_master(self, obj, obj_type, qname):

        body = {'object': obj.as_dict(),
                'type'  : obj_type}

        if   obj_type == 'Task' : msg = obj.parent_stage['uid']
        elif obj_type == 'Stage': msg = obj.parent_pipeline['uid']
        else                    : msg = ''

        self._prof.prof('pub_sync', state=obj.state, uid=obj.uid, msg=msg)
        self._log.debug('%s (%s) to sync with amgr', obj.uid, obj.state)
        self._zmq_queue['put'].put(msgs=[body], qname=qname)


    # --------------------------------------------------------------------------
    #
    def _advance(self, obj, obj_type, new_state, qname):

        old_state = obj.state

        try:

            obj.state = new_state

            if   obj_type == 'Task' : msg = obj.parent_stage['uid']
            elif obj_type == 'Stage': msg = obj.parent_pipeline['uid']
            else                    : msg = None

            self._prof.prof('advance', uid=obj.uid, state=obj.state, msg=msg)
            self._log.info('Transition %s to %s', obj.uid, new_state)

            self._sync_with_master(obj, obj_type, qname)

        except Exception as ex:
            self._log.exception('Transition %s to state %s failed, error: %s',
                                obj.uid, new_state, ex)
            obj.state = old_state
            self._sync_with_master(obj, obj_type, qname)
            raise EnTKError(ex) from ex


    # --------------------------------------------------------------------------
    #
    def start_manager(self):
        """
        **Purpose**: Method to start the tmgr thread. The tmgr function
        is not to be accessed directly. The function is started in a separate
        thread using this method.
        """

        raise NotImplementedError('start_manager() method not implemented in '
                                  'TaskManager for %s' % self._rts)


    # --------------------------------------------------------------------------
    #
    def terminate_manager(self):
        """
        **Purpose**: Method to terminate the tmgr thread. This method is
        blocking as it waits for the tmgr thread to terminate (aka join).
        """

        try:
            if self._tmgr_thread:
                self._log.debug('Trying to terminate task manager.')
                if self._tmgr_terminate is not None:
                    if not self._tmgr_terminate.is_set():
                        self._tmgr_terminate.set()
                    self._log.debug('TMGR terminate is set %s',
                                    self._tmgr_terminate.is_set())
                if self.check_manager():
                    self._log.debug('TMGR thread is alive')
                    self._tmgr_thread.join(30)
                self._log.debug('TMGR thread joined')
                self._tmgr_thread = None

                self._log.info('Task manager thread closed')
                self._prof.prof('tmgr_term', uid=self._uid)

        except Exception as ex:
            self._log.exception('Could not terminate task manager thread')
            raise EnTKError(ex) from ex


    # --------------------------------------------------------------------------
    #
    def check_manager(self):
        """
        **Purpose**: Check if the tmgr thread is alive and running
        """

        # Return False if the thread does not exist
        if self._tmgr_thread:
            return self._tmgr_thread.is_alive()
        else:
            return False


# ------------------------------------------------------------------------------

