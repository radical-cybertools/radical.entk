
__copyright__ = 'Copyright 2017-2018, http://radical.rutgers.edu'
__author__    = 'Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>'
__license__   = 'MIT'

import os
import json
import pika
import time

import threading     as mt

import radical.utils as ru

from .. import exceptions as ree

from ..pipeline    import Pipeline
from ..task        import Task
from radical.entk  import states
from ..utils       import write_session_description
from ..utils       import write_workflows

from .wfprocessor  import WFprocessor


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
        :hostname:        host rabbitmq server is running
        :port:            port at which rabbitmq can be accessed
        :username:        username to log in to RabbitMQ
        :password:        password to log in to RabbitMQ
        :reattempts:      number of attempts to re-invoke any failed EnTK
                          components
        :resubmit_failed: resubmit failed tasks (True/False)
        :autoterminate:   terminate resource reservation upon execution of all
                          tasks of first workflow (True/False)
        :write_workflow:  write workflow and mapping to rts entities to a file
                          (post-termination)
        :rts:             Specify RTS to use. Current options: 'mock',
                          'radical.pilot' (default if unspecified)
        :rmq_cleanup:     Cleanup all queues created in RabbitMQ server for
                          current execution (default is True)
        :rts_config:      Configuration for the RTS, accepts
                          {'sandbox_cleanup': True/False,'db_cleanup':
                          True/False} when RTS is RP
        :name:            Name of the Application. It should be unique between
                          executions. (default is randomly assigned)
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self,
                 config_path=None,
                 hostname=None,
                 port=None,
                 username=None,
                 password=None,
                 reattempts=None,
                 resubmit_failed=None,
                 autoterminate=None,
                 write_workflow=None,
                 rts=None,
                 rmq_cleanup=None,
                 rts_config=None,
                 name=None):

        # Create a session for each EnTK script execution
        if name:
            self._name = name
            self._sid  = name
        else:
            self._name = str()
            self._sid  = ru.generate_id('re.session', ru.ID_PRIVATE)

        self._read_config(config_path, hostname, port, username, password,
                          reattempts, resubmit_failed, autoterminate,
                          write_workflow, rts, rmq_cleanup, rts_config)

        # Create an uid + logger + profiles for AppManager, under the sid
        # namespace

        self._uid = ru.generate_id('appmanager.%(counter)04d', ru.ID_CUSTOM)

        path = os.getcwd() + '/' + self._sid
        name = 'radical.entk.%s' % self._uid

        self._logger = ru.Logger(name=name, path=path)
        self._prof   = ru.Profiler(name=name, path=path)
        self._report = ru.Reporter(name=name)

        self._report.info('EnTK session: %s\n' % self._sid)
        self._report.info('Creating AppManager')
        self._prof.prof('amgr_creat', uid=self._uid)

        self._rmgr            = None
        self._pending_queue   = list()
        self._completed_queue = list()

        # Global parameters to have default values
        self._mqs_setup       = False
        self._resource_desc   = None
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
        self._port            = int(self._port)

        # Setup rabbitmq queues
        self._setup_mqs()

        self._rmq_ping_interval = int(os.getenv('RMQ_PING_INTERVAL', "10"))

        self._logger.info('Application Manager initialized')
        self._prof.prof('amgr_created', uid=self._uid)
        self._report.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def _read_config(self, config_path, hostname, port, username, password,
                     reattempts, resubmit_failed, autoterminate,
                     write_workflow, rts, rmq_cleanup, rts_config):

        if not config_path:
            config_path = os.path.dirname(os.path.abspath(__file__))

        config = ru.read_json(os.path.join(config_path, 'config.json'))

        def _if(val1, val2):
            if val1 is not None: return val1
            else               : return val2

        self._hostname         = _if(hostname,        config['hostname'])
        self._port             = _if(port,            config['port'])
        self._username         = _if(username,        config['username'])
        self._password         = _if(password,        config['password'])
        self._reattempts       = _if(reattempts,      config['reattempts'])
        self._resubmit_failed  = _if(resubmit_failed, config['resubmit_failed'])
        self._autoterminate    = _if(autoterminate,   config['autoterminate'])
        self._write_workflow   = _if(write_workflow,  config['write_workflow'])
        self._rmq_cleanup      = _if(rmq_cleanup,     config['rmq_cleanup'])
        self._rts_config       = _if(rts_config,      config['rts_config'])
        self._rts              = _if(rts,             config['rts'])

        credentials = pika.PlainCredentials(self._username, self._password)
        self._rmq_conn_params = pika.connection.ConnectionParameters(
                                        host=self._hostname,
                                        port=self._port,
                                        credentials=credentials)

        # TODO: Pass these values also as parameters
        self._num_pending_qs   = config['pending_qs']
        self._num_completed_qs = config['completed_qs']

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
        :getter: Returns the resource description
        :setter: Assigns a resource description
        '''

        return self._resource_desc


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
            raise ree.TypeError(expected_type=str, actual_type=type(value))

        self._name = value


    # --------------------------------------------------------------------------
    #
    @resource_desc.setter
    def resource_desc(self, value):

        if self._rts == 'radical.pilot':
            from radical.entk.execman.rp import ResourceManager

        elif self._rts == 'mock':
            from radical.entk.execman.mock import ResourceManager

        self._rmgr = ResourceManager(resource_desc=value, sid=self._sid,
                                     rts_config=self._rts_config)

        self._report.info('Validating and assigning resource manager')

        if not self._rmgr._validate_resource_desc():
            self._logger.error('Could not validate resource description')
            raise ree.EnTKError('Could not validate resource description')

        self._rmgr._populate()
        self._rmgr.shared_data = self._shared_data
        self._rmgr.outputs     = self._outputs

        self._report.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    @workflow.setter
    def workflow(self, workflow):

        self._prof.prof('assigning workflow', uid=self._uid)

        for p in workflow:

            if not isinstance(p, Pipeline):
                self._logger.info('workflow type incorrect')
                raise ree.TypeError(expected_type=['Pipeline',
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
                raise ree.TypeError(expected_type=str,
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
                raise ree.TypeError(expected_type=str,
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
                self._logger.error('No workflow assignedcurrently, please \
                                    check your script')
                raise ree.MissingError(obj=self._uid,
                                       missing_attribute='workflow')

            if not self._rmgr:
                self._logger.error('No resource manager assigned currently, \
                                    please create and add a valid resource \
                                    manager')
                raise ree.MissingError(obj=self._uid,
                                   missing_attribute='resource_manager')
            self._prof.prof('amgr run started', uid=self._uid)

            # ensure rabbitmq setup
            self._setup_mqs()

            # Submit resource request if no resource allocation done till now or
            # resubmit a new one if the old one has completed
            res_alloc_state = self._rmgr.get_resource_allocation_state()
            if not res_alloc_state or \
                   res_alloc_state in self._rmgr.get_completed_states():

                self._logger.info('Starting resource request submission')
                self._prof.prof('rreq_init', uid=self._uid)

                self._rmgr._submit_resource_request()

                res_alloc_state = self._rmgr.get_resource_allocation_state()
                if res_alloc_state in self._rmgr.get_completed_states():
                    raise ree.EnTKError(msg='Cannot proceed. Resource '
                                        'ended in state %s' % res_alloc_state)


            # Start all components and subcomponents
            self._start_all_comps()

            # Run workflow -- this call is blocking till all tasks of the
            # workflow are executed or an error/exception is encountered
            self._run_workflow()
            self._logger.info('Workflow execution finished.')
            if self._autoterminate:
                self._logger.debug('Autoterminate set to %s.' % self._autoterminate)
                self.terminate()

        except KeyboardInterrupt:

            self._logger.exception('Execution interrupted by user (you '
                                   'probably hit Ctrl+C), trying to cancel '
                                   'enqueuer thread gracefully...')
            self.terminate()
            raise

        except Exception:

            self._logger.exception('Error in AppManager')
            self.terminate()
            raise

        # return list of fetched output data, or None.
        outputs = self.outputs
        if outputs:
            ret = outputs
        return ret


    # --------------------------------------------------------------------------
    #
    def terminate(self):

        self._prof.prof('term_start', uid=self._uid)

        # Terminate threads in following order: wfp, helper, synchronizer
        if self._wfp:
            self._logger.info('Terminating WFprocessor')
            self._wfp.terminate_processor()

        if self._task_manager:
            self._logger.info('Terminating task manager process')
            self._task_manager.terminate_manager()
            self._task_manager.terminate_heartbeat()

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

        if self._rmq_cleanup:
            self._cleanup_mqs()

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
    def _setup_mqs(self):
        '''
        **Purpose**: Setup RabbitMQ system on the client side. We instantiate
        queue(s) 'pendingq-*' for communication between the enqueuer thread and
        the task manager process. We instantiate queue(s) 'completedq-*' for
        communication between the task manager and dequeuer thread. We
        instantiate queue 'sync-to-master' for communication from
        enqueuer/dequeuer/task_manager to the synchronizer thread. We
        instantiate queue 'sync-ack' for communication from synchronizer thread
        to enqueuer/dequeuer/task_manager.

        Details: All queues are durable: Even if the RabbitMQ server goes down,
        the queues are saved to disk and can be retrieved. This also means that
        after an erroneous run the queues might still have unacknowledged
        messages and will contain messages from that run. Hence, in every new
        run, we first delete the queue and create a new one.
        '''

        try:
            self._report.info('Setting up RabbitMQ system')
            if self._mqs_setup:
                self._report.ok('>>n/a\n')
                return

            self._report.ok('>>ok\n')

            self._prof.prof('mqs_setup_start', uid=self._uid)
            self._logger.debug('Setting up mq connection and channel')

            mq_connection = pika.BlockingConnection(self._rmq_conn_params)

            mq_channel = mq_connection.channel()

            self._logger.debug('Connection and channel setup successful')
            self._logger.debug('Setting up all exchanges and queues')

            qs = ['%s-tmgr-to-sync' % self._sid,
                  '%s-cb-to-sync'   % self._sid,
                  '%s-sync-to-tmgr' % self._sid,
                  '%s-sync-to-cb'   % self._sid]

            for i in range(1, self._num_pending_qs + 1):
                queue_name = '%s-pendingq-%s' % (self._sid, i)
                self._pending_queue.append(queue_name)
                qs.append(queue_name)

            for i in range(1, self._num_completed_qs + 1):
                queue_name = '%s-completedq-%s' % (self._sid, i)
                self._completed_queue.append(queue_name)
                qs.append(queue_name)

          # f = open('.%s.txt' % self._sid, 'w')
            for q in qs:
                # Durable Qs will not be lost if rabbitmq server crashes
                mq_channel.queue_declare(queue=q)
          #     f.write(q + '\n')
          # f.close()

            self._mqs_setup = True

            self._logger.debug('All exchanges and queues are setup')
            self._prof.prof('mqs_setup_stop', uid=self._uid)

        except Exception as ex:

            self._logger.exception('Error setting RabbitMQ system: %s' % ex)
            raise


    # --------------------------------------------------------------------------
    #
    def _cleanup_mqs(self):

        try:
            self._prof.prof('mqs_cleanup_start', uid=self._uid)

            mq_connection = pika.BlockingConnection(self._rmq_conn_params)
            mq_channel = mq_connection.channel()

            mq_channel.queue_delete(queue='%s-tmgr-to-sync' % self._sid)
            mq_channel.queue_delete(queue='%s-cb-to-sync' % self._sid)
            mq_channel.queue_delete(queue='%s-sync-to-tmgr' % self._sid)
            mq_channel.queue_delete(queue='%s-sync-to-cb' % self._sid)

            for i in range(1, self._num_pending_qs + 1):
                queue_name = '%s-pendingq-%s' % (self._sid, i)
                mq_channel.queue_delete(queue=queue_name)

            for i in range(1, self._num_completed_qs + 1):
                queue_name = '%s-completedq-%s' % (self._sid, i)
                mq_channel.queue_delete(queue=queue_name)

            self._prof.prof('mqs_cleanup_stop', uid=self._uid)

            self._mqs_setup = False

        except Exception:
            self._logger.exception('Message queues not deleted, error')
            raise


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
                                pending_queue=self._pending_queue,
                                completed_queue=self._completed_queue,
                                resubmit_failed=self._resubmit_failed,
                                rmq_conn_params=self._rmq_conn_params)
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
            from radical.entk.execman.rp import TaskManager

        elif self._rts == 'mock':
            from radical.entk.execman.mock import TaskManager

        if not self._task_manager:

            self._logger.info('Starting task manager')
            self._prof.prof('tmgr_create_start', uid=self._uid)

            self._task_manager = TaskManager(
                    sid=self._sid,
                    pending_queue=self._pending_queue,
                    completed_queue=self._completed_queue,
                    rmgr=self._rmgr,
                    rmq_conn_params=self._rmq_conn_params)

            self._task_manager.start_manager()
            self._task_manager.start_heartbeat()

            self._prof.prof('tmgr_create_stop', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _run_workflow(self):

        active_pipe_count  = len(self._workflow)
        finished_pipe_uids = list()

        # We wait till all pipelines of the workflow are marked
        # complete
        state      = self._rmgr.get_resource_allocation_state()
        final      = self._rmgr.get_completed_states()
        incomplete = self._wfp.workflow_incomplete()

        while active_pipe_count and \
              incomplete        and \
              state not in final:

            state = self._rmgr.get_resource_allocation_state()

            for pipe in self._workflow:

                with pipe.lock:

                    if pipe.completed and \
                        pipe.uid not in finished_pipe_uids:

                        finished_pipe_uids.append(pipe.uid)
                        active_pipe_count -= 1

                        self._logger.info('Pipe %s completed' % pipe.uid)
                        self._logger.info('Active pipes %s' % active_pipe_count)


            if not self._sync_thread.is_alive() and \
                self._cur_attempt <= self._reattempts:

                self._sync_thread = mt.Thread(target=self._synchronizer,
                                              name='synchronizer-thread')
                self._sync_thread.start()
                self._cur_attempt += 1

                self._prof.prof('sync_thread_restart', uid=self._uid)
                self._logger.info('Restarting synchronizer thread')


            if not self._wfp.check_processor() and \
                self._cur_attempt <= self._reattempts:

                # If WFP dies, both child threads are also cleaned out.
                # We simply recreate the wfp object with a copy of the
                # workflow in the appmanager and start the processor.

                self._prof.prof('wfp_recreate', uid=self._uid)
                self._wfp = WFprocessor(sid=self._sid,
                                        workflow=self._workflow,
                                        pending_queue=self._pending_queue,
                                        completed_queue=self._completed_queue,
                                        resubmit_failed=self._resubmit_failed,
                                        rmq_conn_params=self._rmq_conn_params)

                self._logger.info('Restarting WFProcessor')
                self._wfp.start_processor()

                self._cur_attempt += 1


            if not self._task_manager.check_heartbeat() and \
                self._cur_attempt <= self._reattempts:

                # If the tmgr process or heartbeat dies, we simply start a
                # new process using the start_manager method. We do not
                # need to create a new instance of the TaskManager object
                # itself. We stop and start a new instance of the
                # heartbeat thread as well.

                self._prof.prof('restart_tmgr', uid=self._uid)

                self._logger.info('Terminating heartbeat thread')
                self._task_manager.terminate_heartbeat()
                self._logger.info('Terminating tmgr process')
                self._task_manager.terminate_manager()

                self._logger.info('Restarting task manager process')
                self._task_manager.start_manager()
                self._logger.info('Restarting heartbeat thread')
                self._task_manager.start_heartbeat()

                self._cur_attempt += 1


    # --------------------------------------------------------------------------
    #
    def _get_message_to_sync(self, mq_channel, qname):
        '''
        Reads a message from the queue, and exchange the message to where it
        was published by `update_task`
        '''

        # --------------------------------------------------------------
        # Messages between tmgr Main thread and synchronizer -- only
        # Task objects
        method_frame, props, body = mq_channel.basic_get(queue=qname)
        tmp = qname.split("-")
        q_sid = ''.join(tmp[:-3])
        q_from = tmp[-3]
        q_to = tmp[-1]
        return_queue_name = f"{q_sid}-{q_to}-to-{q_from}"

        # The message received is a JSON object with the following
        # structure:
        # msg = {
        #         'type': 'Pipeline'/'Stage'/'Task',
        #         'object': json/dict
        #         }
        if body:

            msg   = json.loads(body)
            uid   = msg['object']['uid']
            state = msg['object']['state']

            self._prof.prof('sync_recv_obj_state_%s' % state, uid=uid)
            self._logger.debug('recv %s in state %s (sync)' % (uid, state))

            if msg['type'] == 'Task':
                self._update_task(msg, return_queue_name, props.correlation_id,
                        mq_channel, method_frame)


    # --------------------------------------------------------------------------
    #
    def _update_task(self, msg, reply_to, corr_id, mq_channel, method_frame):
        # pylint: disable=W0612,W0613

        completed_task = Task()
        completed_task.from_dict(msg['object'])

        self._logger.info('Received %s with state %s'
                         % (completed_task.uid, completed_task.state))

      # found_task = False

        # Traverse the entire workflow to find the correct task
        for pipe in self._workflow:

            with pipe.lock:

                if pipe.completed or \
                    pipe.uid != completed_task.parent_pipeline['uid']:
                    continue

                for stage in pipe.stages:

                    if stage.uid != completed_task.parent_stage['uid']:
                        continue

                    for task in stage.tasks:

                        if completed_task.uid != task.uid or \
                            completed_task.state == task.state:
                            continue

                        self._logger.debug(('Found task %s in state (%s)'
                            ' changing to %s ==') %
                                (task.uid, task.state, completed_task.state))

                        if completed_task.path:
                            task.path = str(completed_task.path)
                            self._logger.debug('Task %s path set to %s' %
                                               (task.uid, task.path))

                        if task.state in [states.DONE, states.FAILED]:
                            self._logger.debug(('No change on task state %s '
                                'in state %s') % (task.uid, task.state))
                            break
                        task.state = str(completed_task.state)
                        self._logger.debug('Found task %s in state %s'
                                          % (task.uid, task.state))

                        # mq_channel.basic_publish(
                        #        exchange='',
                        #        routing_key=reply_to,
                        #        properties=pika.BasicProperties(
                        #            correlation_id=corr_id),
                        #        body='%s-ack' % task.uid)

                        state = msg['object']['state']
                        self._prof.prof('pub_ack_state_%s' % state,
                                        uid=msg['object']['uid'])

                        mq_channel.basic_ack(
                                delivery_tag=method_frame.delivery_tag)

                        self._report.ok('Update: ')
                        self._report.info('%s state: %s\n'
                                         % (task.luid, task.state))

                      # found_task = True
                        break

                # if not found_task:
                #
                #     # If there was a Stage update, but the Stage was
                #     # not found in any of the Pipelines. This
                #     # means that this was a Stage that was added
                #     # during runtime and the AppManager does not
                #     # know about it. The current solution is going
                #     # to be: add it to the workflow object in the
                #     # AppManager via the synchronizer.
                #
                #     self._logger.info('Adding new task %s to \
                #                         parent stage: %s'
                #                         % (completed_task.uid,
                #                         stage.uid))
                #
                #     self._prof.prof('adap_add_task_start',
                #                     uid=completed_task.uid)
                #     stage.add_tasks(completed_task)
                #     self._prof.prof('adap_add_task_stop',
                #                     uid=completed_task.uid)
                #
                #     mq_channel.basic_publish(exchange='',
                #                 routing_key=reply_to,
                #                 properties=pika.BasicProperties(
                #                     correlation_id=corr_id),
                #                 body='%s-ack' % completed_task.uid)
                #
                #     self._prof.prof('pub_ack_state_%s' %
                #                 msg['object']['state'],
                #                 uid=msg['object']['uid'])
                #
                #     mq_channel.basic_ack(
                #         delivery_tag=method_frame.delivery_tag)
                #     self._report.ok('Update: ')
                #     self._report.info('%s state: %s\n' %
                #     (completed_task.luid, completed_task.state))


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

        except Exception:
            self._logger.exception('Unknown error in synchronizer: %s. \
                                    Terminating thread')
            raise


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

        mq_connection = pika.BlockingConnection(self._rmq_conn_params)
        mq_channel = mq_connection.channel()

        last  = time.time()
        qname_t2s = '%s-tmgr-to-sync' % self._sid
        qname_c2s = '%s-cb-to-sync'   % self._sid

        while not self._terminate_sync.is_set():

            # wrapper to call `_update_task()`
            self._get_message_to_sync(mq_channel, qname_t2s)
            self._get_message_to_sync(mq_channel, qname_c2s)

            # Appease pika cos it thinks the connection is dead
            now = time.time()
            if now - last >= self._rmq_ping_interval:
                mq_connection.process_data_events()
                last = now

        self._prof.prof('sync_thread_stop', uid=self._uid)


# ------------------------------------------------------------------------------
# pylint: disable=protected-access

