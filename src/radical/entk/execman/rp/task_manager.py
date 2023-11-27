# pylint: disable=protected-access

__copyright__ = 'Copyright 2017-2018, http://radical.rutgers.edu'
__author__    = 'Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>'
__license__   = 'MIT'


import os
import queue
import pickle

import threading       as mt

import radical.pilot   as rp

from ...exceptions       import EnTKError
from ...                 import states, Task
from ..base              import Base_TaskManager
from .task_processor     import create_td_from_task, create_task_from_rp


# ------------------------------------------------------------------------------
#
class TaskManager(Base_TaskManager):
    '''
    A Task Manager takes the responsibility of dispatching tasks it receives
    from a queue for execution on to the available resources using a runtime
    system. In this case, the runtime system being used RADICAL Pilot. Once
    the tasks have completed execution, they are pushed on to another queue for
    other components of EnTK to access.


    :arguments:
        :rmgr:              (ResourceManager) Object to be used to access the
                            Pilot where the tasks can be submitted

    Currently, EnTK is configured to work with one pending queue and one
    completed queue. In the future, the number of queues can be varied for
    different throughput requirements at the cost of additional Memory and CPU
    consumption.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, sid, rmgr, zmq_info):

        super(TaskManager, self).__init__(sid, rmgr, rts='radical.pilot',
                                          zmq_info=zmq_info)
        self._rts_runner = None
        self._zmq_info   = zmq_info

        self._log.info('Created task manager object: %s', self._uid)
        self._prof.prof('tmgr_create', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _tmgr(self, uid, rmgr, zmq_info):
        '''
        **Purpose**: This method has 2 purposes: receive tasks from the
                     'pending' queue, start a new thread that processes these
                     tasks and submits to the RTS.

                     The new thread is responsible for pushing completed tasks
                     (returned by the RTS) to the dequeueing queue. It also
                     converts Tasks into TDs and CUs into (partially described)
                     Tasks. This conversion is necessary since the current RTS
                     is RADICAL Pilot. Once Tasks are recovered from a CU, they
                     are then pushed to the completed queue. At all state
                     transitions, they are synced (blocking) with the
                     AppManager in the master process.

        **Details**: The AppManager can re-invoke the tmgr thread with this
                     function if the execution of the workflow is still
                     incomplete. There is also population of a dictionary,
                     `placeholders`, which stores the path of each of the
                     tasks on the remote machine.
        '''

        session = rp.Session(uid=self._sid,
                             _reg_addr=rmgr.pmgr.cfg.reg_addr,
                             _role=rp.Session._DEFAULT)
        # FIXME: use a non-primary session for rp.TaskManager
        session._role = rp.Session._PRIMARY
        tmgr = rp.TaskManager(session=session)

        self._submitted_tasks = dict()
        self._total_res       = {'cores': 0, 'gpus': 0}

        try:

            self._setup_zmq(zmq_info)

            self._prof.prof('tmgr thread started', uid=self._uid)
            self._log.info('Task Manager thread started')

            # Queue for communication between threads of this thread
            task_queue     = queue.Queue()
            tasks_per_item = 1000  # sets a size of item for `Queue.put`

            # Pickle file for task id history.
            # TODO: How do you take care the first execution.
            pkl_path = self._path + '/.task_submitted.pkl'
            if os.path.exists(pkl_path):
                with open(pkl_path, 'rb') as f:
                    self._submitted_tasks = pickle.load(f)

            # Start a thread to receive tasks and push to RTS
            self._rts_runner = mt.Thread(target=self._process_tasks,
                                         args=(task_queue, tmgr))
            self._rts_runner.daemon = True
            self._rts_runner.start()

            self._prof.prof('tmgr infrastructure setup done', uid=uid)

            # While we are supposed to run and the thread that does the work is
            # alive go.
            while not self._tmgr_terminate.is_set() and \
                      self._rts_runner.is_alive():

                try:

                    msgs = self._zmq_queue['get'].get_nowait(
                            qname='pending', timeout=100)

                    if msgs:

                        for msg in msgs:

                            if msg['type'] == 'workload':

                                tasks = msg['body']
                                self._log.debug('Task queue: ready workload '
                                                'with %s task(s)', len(tasks))

                                queue_items = [
                                    tasks[i:i + tasks_per_item] for i in
                                    range(0, len(tasks), tasks_per_item)]

                                for item in queue_items:
                                    task_queue.put(item)

                            elif msg['type'] == 'rts':
                                self._update_resource(msg['body'], tmgr)

                            else:
                                self._log.error('TMGR receiver wrong message type')

                except Exception as e:
                    self._log.exception('Error in task execution')
                    raise EnTKError(e) from e

            self._log.debug('Exited TMGR main loop')

        except KeyboardInterrupt:

            self._log.exception('Execution interrupted (probably by Ctrl+C), '
                                'cancel tmgr thread gracefully...')
            raise


        except Exception as e:

            self._log.exception('task %s failed')
            raise EnTKError(e) from e

        finally:

            self._prof.prof('tmgr_term', uid=uid)

            if self._rts_runner:
                self._rts_runner.join()
            tmgr.close()

            self._log.debug('TMGR RTS Runner joined')

            self._prof.close()
            self._log.debug('TMGR profile closed')


    # --------------------------------------------------------------------------
    #
    def _update_resource(self, pilot, tmgr):
        '''
        Update used pilot.
        '''

        # Busy wait unit RP TMGR exists. Does some other way make sense?
        self._log.debug('Adding pilot.')

        curr_pilot = tmgr.list_pilots()
        if curr_pilot:
            self._log.debug('Got old pilots')
            tmgr.remove_pilots(pilot_ids=curr_pilot)
        tmgr.add_pilots(pilot)

        self._total_res = {'cores': pilot['description']['cores'],
                           'gpus' : pilot['description']['gpus']}
        self._log.debug('Added new pilot')


    # --------------------------------------------------------------------------
    #
    def _process_tasks(self, task_queue, tmgr):
        '''
        **Purpose**: The new thread that gets spawned by the main tmgr thread
                     invokes this function. This function receives tasks from
                     'task_queue' and submits them to the RADICAL Pilot RTS.
        '''

        placeholders = dict()
        placeholders['__by_name__'] = dict()
        placeholders_by_name = placeholders['__by_name__']
        placeholder_lock = mt.Lock()

        # ----------------------------------------------------------------------
        def load_placeholder(task):
            with placeholder_lock:
                parent_pipeline = str(task.parent_pipeline['uid'])
                parent_stage = str(task.parent_stage['uid'])

                if parent_pipeline not in placeholders:
                    placeholders[parent_pipeline] = dict()

                if parent_stage not in placeholders[parent_pipeline]:
                    placeholders[parent_pipeline][parent_stage] = dict()

                if None not in [parent_pipeline, parent_stage, task.uid]:
                    placeholders[parent_pipeline][parent_stage][task.uid] = \
                                                            {'path': task.path,
                                                             'uid': task.uid}

                parent_pipeline_n = str(task.parent_pipeline['name'])
                parent_stage_n = str(task.parent_stage['name'])

                if parent_pipeline_n not in placeholders_by_name:
                    placeholders_by_name[parent_pipeline_n] = dict()

                if parent_stage_n not in placeholders_by_name[parent_pipeline_n]:
                    placeholders_by_name[parent_pipeline_n][parent_stage_n] = dict()

                if None not in [parent_pipeline_n, parent_stage_n, task.name]:
                    placeholders_by_name[parent_pipeline_n][parent_stage_n][task.name] = \
                                                            {'path': task.path,
                                                             'uid': task.uid}

        # ----------------------------------------------------------------------
        def task_state_cb(rp_task, state):

            try:
                self._log.debug('Task %s in state %s', rp_task.uid,
                                                       rp_task.state)

                if rp_task.state in rp.FINAL:

                    task = create_task_from_rp(rp_task, self._log, self._prof)

                    self._advance(task, 'Task', states.COMPLETED, 'cb-to-sync')

                    load_placeholder(task)

                    tdict = task.as_dict()

                    self._zmq_queue['put'].put(qname='completed', msgs=[tdict])
                    self._log.info('Pushed task %s with state %s to completed',
                                   task.uid, task.state)

            except KeyboardInterrupt as ex:
                self._log.exception('Execution interrupted (probably by Ctrl+C)'
                                    ' exit callback thread gracefully...')
                raise KeyboardInterrupt(ex) from ex

            except Exception as ex:
                self._log.exception('Error in RP callback thread: %s', ex)
                raise EnTKError(ex) from ex
        # ----------------------------------------------------------------------

        tmgr.register_callback(task_state_cb)

        try:
            pkl_path = self._path + '/.task_submitted.pkl'

            while not self._tmgr_terminate.is_set():

                try:
                    body = task_queue.get(block=True, timeout=10)
                except queue.Empty:
                    # Ignore, we don't always have new tasks to run
                    continue

                task_queue.task_done()
                self._log.debug('Task queue: got workload with '
                                '%s task(s)', len(body or []))

                if not body:
                    continue

                bulk_tasks = list()

                for msg in body:

                    task = Task(from_dict=msg)
                    load_placeholder(task)
                    bulk_tasks.append(create_td_from_task(
                                        task, placeholders,
                                        self._submitted_tasks, pkl_path,
                                        self._sid, self._log, self._prof))

                    self._advance(task, 'Task', states.SUBMITTING,
                                      'tmgr-to-sync')

                if bulk_tasks:
                    tmgr.submit_tasks(bulk_tasks)

            self._log.debug('Exited RTS main loop. TMGR terminating')
        except KeyboardInterrupt:
            self._log.exception('Execution interrupted (probably by Ctrl+C), '
                                'cancel task processor gracefully...')
            raise

        except Exception as e:
            self._log.exception('%s failed', self._uid)
            raise EnTKError(e) from e


    # --------------------------------------------------------------------------
    #
    def start_manager(self):
        '''
        **Purpose**: Method to start the tmgr thread. The tmgr function
                     is not to be accessed directly. The function is started
                     in a separate thread using this method.
        '''
        # pylint: disable=attribute-defined-outside-init, access-member-before-definition
        if self._tmgr_thread:
            self._log.warn('tmgr thread already running!')
            return

        try:
            self._prof.prof('creating tmgr thread', uid=self._uid)
            self._tmgr_terminate = mt.Event()

            # preserve session before forking
            session = self._rmgr.session
            self._rmgr._session = None

            self._tmgr_thread = mt.Thread(target=self._tmgr,
                                          name='task-manager',
                                          args=(self._uid,
                                                self._rmgr,
                                                self._zmq_info))
            self._tmgr_thread.daemon = True

            self._rmgr._session = session

            self._log.info('Starting task manager thread')
            self._prof.prof('starting tmgr thread', uid=self._uid)

            self._tmgr_thread.start()
            self._log.debug('tmgr thread "task-manager" started')

            return True

        except Exception as e:

            self._log.exception('Task manager not started')
            self.terminate_manager()
            raise EnTKError(e) from e


# ------------------------------------------------------------------------------
# pylint: enable=attribute-defined-outside-init, access-member-before-definition
