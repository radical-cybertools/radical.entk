
__copyright__ = 'Copyright 2017-2018, http://radical.rutgers.edu'
__author__    = 'Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>'
__license__   = 'MIT'

import os
import time
import warnings

import threading       as mt
import multiprocessing as mp

import radical.utils   as ru

from ..exceptions import EnTKError, EnTKTypeError, EnTKMissingError
from ..states     import DONE, FAILED

from ..pipeline   import Pipeline
from ..task       import Task
from ..utils      import write_session_description
from ..utils      import write_workflows

from .wfprocessor import WFprocessor


# pylint: disable=protected-access

# ------------------------------------------------------------------------------
#
class AppManager(object):

    '''
    An application manager takes the responsibility of setting up the
    communication infrastructure, instantiates the ResourceManager, TaskManager,
    WFProcessor objects and all their threads and processes. This is the Master
    object running in the main process and is designed to recover from errors
    from all other objects, threads and processes.

    :Arguments:
        :config_path:     Url to config path to be read for AppManager
        :reattempts:      number of attempts to re-invoke any failed EnTK
                          components
        :resubmit_failed: resubmit failed tasks (True/False)
        :autoterminate:   terminate resource reservation upon execution of all
                          tasks of first workflow (True/False)
        :write_workflow:  write workflow and mapping to rts entities to a file
                          (post-termination)
        :rts:             Specify RTS to use. Current options: 'mock',
                          'radical.pilot' (default if unspecified)
        :rts_config:      Configuration for the RTS, accepts
                          {'sandbox_cleanup': True/False,'db_cleanup':
                          True/False} when RTS is RP
        :name:            Name of the Application. It should be unique between
                          executions. (default is randomly assigned)
        :base_path:       base path of logger and profiler with a session id,
                          None (default) will use a current working directory
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self,
                 config_path=None,
                 reattempts=None,
                 resubmit_failed=None,
                 autoterminate=None,
                 write_workflow=None,
                 rts=None,
                 rts_config=None,
                 name=None,
                 base_path=None,
                 **kwargs):

        for arg in ['hostname', 'port', 'username', 'password']:
            if arg in kwargs:
                warnings.warn('%s argument is not required anymore' % arg,
                              DeprecationWarning, stacklevel=2)

        # Create a session for each EnTK script execution
        if name:
            self._name = name
            self._sid  = name
        else:
            self._name = str()
            self._sid  = ru.generate_id('re.session', ru.ID_PRIVATE)

        self._read_config(config_path, reattempts, resubmit_failed,
                          autoterminate, write_workflow, rts, rts_config,
                          base_path)

        # Create an uid + logger + profiles for AppManager, under the sid
        # namespace

        self._uid = ru.generate_id('appmanager.%(counter)04d', ru.ID_CUSTOM)

        path = str(self._base_path) + '/' + self._sid
        name = 'radical.entk.%s' % self._uid

        self._logger = ru.Logger(name=name, path=path)
        self._prof   = ru.Profiler(name=name, path=path)
        self._report = ru.Reporter(name=name)

        self._report.info('EnTK session: %s\n' % self._sid)
        self._report.info('Creating AppManager\n')
        self._prof.prof('amgr_creat', uid=self._uid)

        self._rmgr            = None

        # Global parameters to have default values
        self._resource_desc   = None
        self._services        = list()
        self._task_manager    = None
        self._workflow        = None
        self._workflows       = list()
        self._cur_attempt     = 1
        self._shared_data     = list()
        self._outputs         = None
        self._wfp             = None
        self._sync_thread     = None
        self._terminate_sync  = mt.Event()
        self._resubmit_failed = False
        self._term            = mp.Event()

        # Setup zmq queues
        self._zmq_info        = dict()
        self._zmq_queue       = None
        self._zmq_bridge      = None
        self._setup_zmq()

        self._prof.prof('amgr_created', uid=self._uid)
        self._report.info('AppManager initialized')
        self._report.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def _read_config(self, config_path, reattempts, resubmit_failed,
                           autoterminate, write_workflow, rts, rts_config,
                           base_path):

        if not config_path:
            config_path = os.path.dirname(os.path.abspath(__file__))

        config = ru.read_json(os.path.join(config_path, 'config.json'))

        def _if(val1, val2):
            if val1 is not None: return val1
            else               : return val2

        self._base_path        = _if(base_path,       os.getcwd())
        self._reattempts       = _if(reattempts,      config['reattempts'])
        self._resubmit_failed  = _if(resubmit_failed, config['resubmit_failed'])
        self._autoterminate    = _if(autoterminate,   config['autoterminate'])
        self._write_workflow   = _if(write_workflow,  config['write_workflow'])
        self._rts_config       = _if(rts_config,      config['rts_config'])
        self._rts              = _if(rts,             config['rts'])

        if self._rts not in ['radical.pilot', 'mock']:
            raise ValueError('invalid RTS %s' % self._rts)


    # --------------------------------------------------------------------------
    #
    # Getter functions
    #
    @property
    def name(self):
        '''
        Name for the application manager. Allows the user to setup the name of
        the application manager, as well as its session ID. This name should be
        unique between different EnTK executions, otherwise it will produce an
        error.

        :getter: Returns the name of the application manager
        :setter: Assigns the name of the application manager
        :type: String
        '''

        return self._name


    # --------------------------------------------------------------------------
    #
    @property
    def sid(self):
        '''
        Get the session ID of the current EnTK execution

        :getter: Returns the session ID of the EnTK execution
        :type: String
        '''

        return self._sid


    # --------------------------------------------------------------------------
    #
    @property
    def resource_desc(self):
        '''
        The resource description is a dictionary that holds information about
        the resource(s) that will be used to execute the workflow.

        The following keys are mandatory in all resource descriptions:
            | 'resource'      : Label of the platform with resources.
            | 'walltime'      : Amount of time the workflow is expected to run.
            | 'cpus'          : Number of CPU cores/threads.

        Optional keys include:
            | 'project'       : The project that will be charged.
            | 'gpus'          : Number of GPUs to be used by the workflow.
            | 'access_schema' : The key of an access mechanism to use.
            | 'queue'         : The name of the batch queue.
            | 'job_name'      : The specific name of a batch job.

        :getter: Returns the resource description
        :setter: Assigns a resource description
        '''

        return self._resource_desc


    # --------------------------------------------------------------------------
    #
    @property
    def services(self):
        '''
        :getter: Returns the list of tasks used to start "global" services
        :setter: Assigns a list of service tasks, which are launched before
        any stage starts and run during the whole workflow execution
        '''

        return self._rmgr.services if self._rmgr else self._services


    # --------------------------------------------------------------------------
    #
    @property
    def workflow(self):
        '''
        :getter: Return the last workflow assigned for execution
        :setter: Assign a new workflow to be executed
        '''

        return self._workflow


    # --------------------------------------------------------------------------
    #
    @property
    def workflows(self):
        """
        :getter: Return a list of workflows assigned for execution
        """

        return self._workflows


    # --------------------------------------------------------------------------
    #
    @property
    def shared_data(self):
        '''
        :getter: Return list of filenames that are shared between multiple tasks
                 of the application
        :setter: Assign a list of names of files that need to be staged to the
                remote machine
        '''

        return self._shared_data


    @property
    def outputs(self):
        '''
        :getter: Return list of filenames that are to be staged out after
                 execution
        :setter: Assign a list of names of files that need to be staged from the
                 remote machine
        '''

        return self._outputs


    # --------------------------------------------------------------------------
    # Setter functions
    #
    @name.setter
    def name(self, value):

        if not isinstance(value, str):
            raise EnTKTypeError(expected_type=str, actual_type=type(value))

        self._name = value


    # --------------------------------------------------------------------------
    #
    @resource_desc.setter
    def resource_desc(self, value):

        if self._rts == 'radical.pilot':
            from ..execman.rp import ResourceManager

        elif self._rts == 'mock':
            from ..execman.mock import ResourceManager

        else:
            raise ValueError('unknown RTS %s' % self._rts)

        self._rmgr = ResourceManager(resource_desc=value, sid=self._sid,
                                     rts_config=self._rts_config)

        self._report.info('Validating and assigning resource manager')

        if not self._rmgr._validate_resource_desc():
            self._logger.error('Could not validate resource description')
            raise EnTKError('Could not validate resource description')

        self._resource_desc = value

        self._rmgr._populate()
        self._rmgr.shared_data = self._shared_data
        self._rmgr.outputs     = self._outputs

        if self._services:
            self._rmgr.services = self._services

        self._report.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    @services.setter
    def services(self, tasks):

        if not isinstance(tasks, list):
            tasks = [tasks]

        for task in tasks:
            if not isinstance(task, Task):
                raise EnTKTypeError(expected_type=Task,
                                    actual_type=type(task))

        self._services = tasks
        if self._rmgr:
            self._rmgr.services = tasks


    # --------------------------------------------------------------------------
    #
    @workflow.setter
    def workflow(self, workflow):

        self._prof.prof('assigning workflow', uid=self._uid)

        for p in workflow:

            if not isinstance(p, Pipeline):
                self._logger.info('workflow type incorrect')
                raise EnTKTypeError(expected_type=['Pipeline',
                                                   'set of Pipelines'],
                                    actual_type=type(p))
            p._validate()

        # keep history
        self._workflows.append(workflow)

        # set current workflow
        self._workflow = workflow
        self._logger.info('Workflow assigned to Application Manager')


    # --------------------------------------------------------------------------
    #
    @shared_data.setter
    def shared_data(self, data):

        if not isinstance(data, list):
            data = [data]

        for value in data:
            if not isinstance(value, str):
                raise EnTKTypeError(expected_type=str,
                                    actual_type=type(value))

        self._shared_data = data

        if self._rmgr:
            self._rmgr.shared_data = data



    # --------------------------------------------------------------------------
    #
    @outputs.setter
    def outputs(self, data):

        if not isinstance(data, list):
            data = [data]

        for value in data:
            if not isinstance(value, str):
                raise EnTKTypeError(expected_type=str,
                                    actual_type=type(value))

        if self._rmgr:
            self._rmgr.outputs = data


    # --------------------------------------------------------------------------
    #
    # Public methods
    #
    def run(self):
        '''
        **Purpose**: Run the application manager. Once the workflow and resource
        manager have been assigned. Invoking this method will start the setting
        up the communication infrastructure, submitting a resource request and
        then submission of all the tasks.
        '''

        ret = None

        try:
            self._prof.prof('amgr_start', uid=self._uid)

            # Set None for variables local to each run
            self._resubmit_failed = False
            self._cur_attempt = 1

            # Ensure that a workflow and a resource description have
            # been defined
            if not self._workflow:
                self._logger.error('No workflow assigned currently, please \
                                    check your script')
                raise EnTKMissingError(obj=self._uid,
                                       missing_attribute='workflow')

            if not self._rmgr:
                self._logger.error('No resource manager assigned currently, \
                                    please create and add a valid resource \
                                    manager')
                raise EnTKMissingError(obj=self._uid,
                                       missing_attribute='resource_manager')
            self._prof.prof('amgr run started', uid=self._uid)

            # ensure zmq setup
            self._setup_zmq()

            # Submit resource request if no resource allocation done till now or
            # resubmit a new one if the old one has completed
            res_alloc_state = self._rmgr.get_resource_allocation_state()
            if not res_alloc_state or \
                   res_alloc_state in self._rmgr.get_completed_states():

                self._logger.info('Starting resource request submission')
                self._prof.prof('rreq_init', uid=self._uid)

                self._rmgr.submit_resource_request()

                res_alloc_state = self._rmgr.get_resource_allocation_state()
                if res_alloc_state in self._rmgr.get_completed_states():
                    raise EnTKError('Cannot proceed. Resource ended in state %s'
                                    % res_alloc_state)


            # Start all components and subcomponents
            self._start_all_comps()

            # Run workflow -- this call is blocking till all tasks of the
            # workflow are executed or an error/exception is encountered
            self._run_workflow()
            self._logger.info('Workflow execution finished.')
            if self._autoterminate:
                self._logger.debug('Autoterminate set to %s',
                                   self._autoterminate)
                self.terminate()

        except KeyboardInterrupt as ex:

            self._logger.exception('Execution interrupted by user (you '
                                   'probably hit Ctrl+C), trying to cancel '
                                   'enqueuer thread gracefully...')
            self.terminate()
            raise EnTKError(ex) from ex

        except Exception as ex:

            self._logger.exception('Error in AppManager')
            self.terminate()
            raise EnTKError(ex) from ex

        # return list of fetched output data, or None.
        outputs = self.outputs
        if outputs:
            ret = outputs
        return ret


    # --------------------------------------------------------------------------
    #
    def terminate(self):

        self._prof.prof('term_start', uid=self._uid)

        self._term.set()

        # Terminate threads in following order: wfp, helper, synchronizer
        if self._wfp:
            self._logger.info('Terminating WFprocessor')
            self._wfp.terminate_processor()

        if self._task_manager:
            self._logger.info('Terminating task manager process')
            self._task_manager.terminate_manager()

        if self._sync_thread:
            self._logger.info('Terminating synchronizer thread')
            self._terminate_sync.set()
            self._sync_thread.join()
            self._logger.info('Synchronizer thread terminated')

        if self._write_workflow:
            write_workflows(self.workflows, self._sid)

        if self._rmgr:
            self._rmgr._terminate_resource_request()

        if os.environ.get('RADICAL_ENTK_PROFILE'):
            write_session_description(self)

        self._report.info('All components terminated\n')
        self._prof.prof('termination done', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def resource_terminate(self):

        self._logger.warning('DeprecationWarning: `resource_terminate()` is '
                             'deprecated, please use `terminate()`')
        self.terminate()


    # --------------------------------------------------------------------------
    #
    # Private methods
    #
    def _setup_zmq(self):
        '''
        **Purpose**: Setup ZMQ system on the client side. We instantiate
        queue(s) 'pendingq-*' for communication between the enqueuer thread and
        the task manager process. We instantiate queue(s) 'completedq-*' for
        communication between the task manager and dequeuer thread. We
        instantiate queue 'sync-to-master' for communication from
        enqueuer/dequeuer/task_manager to the synchronizer thread. We
        instantiate queue 'sync-ack' for communication from synchronizer thread
        to enqueuer/dequeuer/task_manager.
        '''

        try:
            sid = self._sid
            self._report.info('Setting up ZMQ queues')

            if self._zmq_info:
                self._report.ok('>>n/a\n')
                return

            self._report.ok('>>ok\n')

            self._prof.prof('zmq_setup_start', uid=self._uid)
            self._logger.debug('Setting up zmq queues')

            cfg = ru.Config(cfg={'channel'   : sid,
                                 'uid'       : sid,
                                 'path'      : sid,
                                 'type'      : 'queue',
                                 'stall_hwm' : 0,
                                 'bulk_size' : 1})

            self._zmq_bridge = ru.zmq.Queue(sid, cfg)
            self._zmq_bridge.start()
            time.sleep(1)

            zmq_info = {
                    'put': str(self._zmq_bridge.addr_put),
                    'get': str(self._zmq_bridge.addr_get)}

            self._zmq_queue = {
                    'put' : ru.zmq.Putter(sid, url=zmq_info['put'], path=sid),
                    'get' : ru.zmq.Getter(sid, url=zmq_info['get'], path=sid)}

            self._zmq_info = zmq_info


        except Exception as e:

            self._logger.exception('Error setting ZMQ queues')
            raise EnTKError(e) from e


    # --------------------------------------------------------------------------
    #
    def _start_all_comps(self):

        if self._wfp:
            # This condition is called when there are multiple workflows
            # submitted for execution. Amgr.run() was probably called twice.
            # If a WFP exists, we use the same one but with the new workflow.
            # Since WFP (and its threads) and the Amgr share memory, we have
            # to terminate WFP's threads, assign the new workflow and then
            # start the threads again.
            self._wfp.terminate_processor()
            self._wfp._workflow = self._workflow
            self._wfp.start_processor()
            return

        # Create WFProcessor and initialize workflow its contents with
        # uids
        self._prof.prof('wfp_create_start', uid=self._uid)
        self._wfp = WFprocessor(sid=self._sid,
                                workflow=self._workflow,
                                resubmit_failed=self._resubmit_failed,
                                zmq_info=self._zmq_info)
        self._prof.prof('wfp_create_stop', uid=self._uid)

        # Start synchronizer thread AM OK
        if not self._sync_thread:
            self._logger.info('Starting synchronizer thread')
            self._sync_thread = mt.Thread(target=self._synchronizer,
                                          name='synchronizer-thread')
            self._prof.prof('sync_thread_create', uid=self._uid)
            self._sync_thread.start()

        # Start WFprocessor
        self._logger.info('Starting WFProcessor')
        self._wfp.start_processor()
        self._report.ok('All components created\n')

        # Create tmgr object only if it does not already exist
        if self._rts == 'radical.pilot':
            from ..execman.rp import TaskManager

        elif self._rts == 'mock':
            from ..execman.mock import TaskManager

        else:
            raise ValueError('unknown RTS %s' % self._rts)

        if not self._task_manager:

            self._logger.info('Starting task manager')
            self._prof.prof('tmgr_create_start', uid=self._uid)

            self._task_manager = TaskManager(
                    sid=self._sid,
                    rmgr=self._rmgr,
                    zmq_info=self._zmq_info)

            self._task_manager.start_manager()
            self._submit_rts_tmgr(self._rmgr.get_rts_info())
            self._prof.prof('tmgr_create_stop', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _submit_rts_tmgr(self, rts_info):
        '''
        **Purpose**: Update the runtime system information in the task manager
        '''
        rts_msg = {'type': 'rts',
                   'body': rts_info}

        self._zmq_queue['put'].put(qname='pending', msgs=rts_msg)


    # --------------------------------------------------------------------------
    #
    def _run_workflow(self):

        active_pipe_count  = len(self._workflow or [])
        finished_pipe_uids = list()

        # We wait till all pipelines of the workflow are marked
        # complete
        # incomplete = self._wfp.workflow_incomplete()
        rts_final_states = self._rmgr.get_completed_states()

        while active_pipe_count and self._cur_attempt <= self._reattempts:

            if self._term.is_set():
                break

            for pipe in self._workflow:

                with pipe.lock:

                    if pipe.completed and \
                       pipe.uid not in finished_pipe_uids:

                        finished_pipe_uids.append(pipe.uid)
                        active_pipe_count -= 1

                        self._logger.info('Pipe %s completed', pipe.uid)
                        self._logger.info('Active pipes %s', active_pipe_count)

            reset_workflow = False

            if not self._sync_thread.is_alive():
                self._logger.info('Synchronizer thread is not alive.')
                self._sync_thread = mt.Thread(target=self._synchronizer,
                                              name='synchronizer-thread')
                self._sync_thread.start()
                self._prof.prof('sync_thread_restart', uid=self._uid)
                self._logger.info('Restarted synchronizer thread.')

                reset_workflow = True

            if not self._wfp.check_processor():
                self._logger.info('WFP is not alive.')

                # If WFP dies, both child threads are also cleaned out.
                # We simply recreate the wfp object with a copy of the
                # workflow in the appmanager and start the processor.

                self._prof.prof('wfp_recreate', uid=self._uid)
                self._wfp.terminate_processor()
                self._wfp.start_processor()
                self._logger.info('Restarted WFProcessor.')

                reset_workflow = True

            state = self._rmgr.get_resource_allocation_state()
            if state in rts_final_states:

                self._cur_attempt += 1
                if self._cur_attempt > self._reattempts:
                    break

                self._logger.debug('Workflow not done. Resubmitting RTS.')

                self._rmgr.submit_resource_request()
                rts_info = self._rmgr.get_rts_info()
                self._submit_rts_tmgr(rts_info=rts_info)
                self._logger.debug('RTS resubmitted')

                reset_workflow = True

            if reset_workflow:
                self._wfp.reset_workflow()

        if self._cur_attempt > self._reattempts:
            raise EnTKError('Too many failures in synchronizer, wfp or '
                            'task manager')


    # --------------------------------------------------------------------------
    #
    def _get_message_to_sync(self, qname):
        '''
        Reads a message from the queue, and exchange the message to where it
        was published by `update_task`
        '''

        # --------------------------------------------------------------
        # Messages between tmgr Main thread and synchronizer -- only
        # Task objects
        msgs = self._zmq_queue['get'].get_nowait(qname=qname)

        if not msgs:
            return 0

        # The message received has the following
        # structure:
        # msg = {
        #         'type': 'Pipeline'/'Stage'/'Task',
        #         'object': json/dict
        #         }
        for msg in msgs:

            uid   = msg['object']['uid']
            state = msg['object']['state']

            obj = msg['object']

            self._prof.prof('sync_recv_obj_state_%s' % state, uid=uid)
            self._logger.debug('recv %s in state %s (sync)', uid, state)

            if msg['type'] == 'Task':
                self._update_task(obj)

        return 1


    # --------------------------------------------------------------------------
    #
    def _update_task(self, tdict):
        # pylint: disable=W0612,W0613

        completed_task = Task(from_dict=tdict)

        self._logger.info('Received %s with state %s', completed_task.uid,
                completed_task.state)

        # Traverse the entire workflow to find the correct task
        for pipe in self._workflow:

            with pipe.lock:

                if pipe.completed:
                    continue

                if pipe.uid != completed_task.parent_pipeline['uid']:
                    continue

                for stage in pipe.stages:

                    if stage.uid != completed_task.parent_stage['uid']:
                        continue

                    for task in stage.tasks:

                        if completed_task.uid != task.uid:
                            continue

                        if completed_task.state == task.state:
                            continue

                        self._logger.debug('Found task %s in state (%s) \
                                           changing to %s ==', task.uid,
                                           task.state, completed_task.state)

                        if completed_task.path:
                            task.path = str(completed_task.path)
                            self._logger.debug('Task %s path set to %s',
                                               task.uid, task.path)

                        if completed_task.rts_uid:
                            task.rts_uid = str(completed_task.rts_uid)
                            self._logger.debug('Task %s rts_uid set to %s',
                                               task.uid, task.rts_uid)

                        if task.state in [DONE, FAILED]:
                            self._logger.debug('No change on task state %s \
                                             in state %s', task.uid, task.state)
                            break

                        task.state            = completed_task.state
                        task.exception        = completed_task.exception
                        task.exception_detail = completed_task.exception_detail

                        self._logger.debug('Found task %s in state %s',
                                           task.uid, task.state)

                        state = tdict['state']
                        self._prof.prof('pub_ack_state_%s' % state,
                                        uid=tdict['uid'])

                        self._report.ok('Update: ')
                        self._report.info('%s state: %s\n'
                                         % (task.luid, task.state))

                        break


    # --------------------------------------------------------------------------
    #
    def _synchronizer(self):

        try:
            self._synchronizer_work()

        except KeyboardInterrupt:
            self._logger.exception('Execution interrupted by user (you \
                                    probably hit Ctrl+C), trying to terminate \
                                    synchronizer thread gracefully...')
            raise

        except Exception as ex:
            self._logger.exception('Unknown error in synchronizer: %s. \
                                    Terminating thread')
            raise EnTKError(ex) from ex


    # --------------------------------------------------------------------------
    #
    def _synchronizer_work(self):
        '''
        **Purpose**: Thread in the master process to keep the workflow data
                     structure in appmanager up to date. We receive only tasks
                     objects from the task manager.

        Details:     Important to note that acknowledgements of the type
                     `channel.basic_ack()` is an acknowledgement to the server
                     that the msg was received.  This is not to be confused with
                     the Ack sent to the task_manager through the sync-ack
                     queue.
        '''

        self._prof.prof('sync_thread_start', uid=self._uid)
        self._logger.info('synchronizer thread started')

        while not self._terminate_sync.is_set():

            # wrapper to call `_update_task()`
            action  = self._get_message_to_sync('tmgr-to-sync')
            action += self._get_message_to_sync('cb-to-sync'  )

            if not action:
                time.sleep(0.01)

            # Raise an exception while running tests
            ru.raise_on(tag='sync_fail')

        self._prof.prof('sync_thread_stop', uid=self._uid)


# ------------------------------------------------------------------------------
# pylint: enable=protected-access

