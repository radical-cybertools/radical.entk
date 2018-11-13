__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__ = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__ = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk.pipeline.pipeline import Pipeline
from radical.entk.stage.stage import Stage
from radical.entk.task.task import Task
from radical.entk.utils.prof_utils import write_session_description
from radical.entk.utils.prof_utils import write_workflow
from wfprocessor import WFprocessor
import time
import os
import Queue
import pika
import json
from threading import Thread, Event
from radical.entk import states


class AppManager(object):

    """
    An application manager takes the responsibility of setting up the communication infrastructure, instantiates the
    ResourceManager, TaskManager, WFProcessor objects and all their threads and processes. This is the Master object
    running in the main process and is designed to recover from errors from all other objects, threads and processes.

    :Arguments:
        :config_path: Url to config path to be read for AppManager
        :hostname: host rabbitmq server is running
        :port: port at which rabbitmq can be accessed
        :reattempts: number of attempts to re-invoke any failed EnTK components
        :resubmit_failed: resubmit failed tasks (True/False)
        :autoterminate: terminate resource reservation upon execution of all tasks of first workflow (True/False)
        :write_workflow: write workflow and mapping to rts entities to a file (post-termination)
        :rts: Specify RTS to use. Current options: 'mock', 'radical.pilot' (default if unspecified)
        :rmq_cleanup: Cleanup all queues created in RabbitMQ server for current execution (default is True)
        :rts_config: Configuration for the RTS, accepts {"sandbox_cleanup": True/False,"db_cleanup": True/False} when RTS is RP
        :name: Name of the Application. It should be unique between executions. (default is randomly assigned)
    """

    def __init__(self,
                 config_path=None,
                 hostname=None,
                 port=None,
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
            self._sid = name
        else:
            self._name= str()
            self._sid = ru.generate_id('re.session', ru.ID_PRIVATE)

        self._read_config(config_path, hostname, port, reattempts,
                          resubmit_failed, autoterminate, write_workflow,
                          rts, rmq_cleanup, rts_config)

        # Create an uid + logger + profiles for AppManager, under the sid
        # namespace
        path = os.getcwd() + '/' + self._sid
        self._uid = ru.generate_id('appmanager.%(item_counter)04d', ru.ID_CUSTOM, namespace=self._sid)
        self._logger = ru.Logger('radical.entk.%s' % self._uid, path=path, targets=['2','.'])
        self._prof = ru.Profiler(name='radical.entk.%s' % self._uid, path=path)
        self._report = ru.Reporter(name='radical.entk.%s' % self._uid)

        self._report.info('EnTK session: %s\n' % self._sid)
        self._prof.prof('create amgr obj', uid=self._uid)
        self._report.info('Creating AppManager')

        self._resource_manager = None
        # RabbitMQ Queues
        self._pending_queue = list()
        self._completed_queue = list()

        # Global parameters to have default values
        self._mqs_setup = False
        self._resource_desc = None
        self._task_manager = None
        self._workflow = None
        self._cur_attempt = 1
        self._shared_data = list()

        self._rmq_ping_interval = os.getenv('RMQ_PING_INTERVAL', 10)

        self._logger.info('Application Manager initialized')
        self._prof.prof('amgr obj created', uid=self._uid)
        self._report.ok('>>ok\n')

    def _read_config(self, config_path, hostname, port, reattempts,
                     resubmit_failed, autoterminate, write_workflow,
                     rts, rmq_cleanup, rts_config):

        if not config_path:
            config_path = os.path.dirname(os.path.abspath(__file__))

        config = ru.read_json(os.path.join(config_path, 'config.json'))

        self._mq_hostname = hostname if hostname else str(config['hostname'])
        self._port = port if port else config['port']
        self._reattempts = reattempts if reattempts else config['reattempts']
        self._resubmit_failed = resubmit_failed if resubmit_failed is not None else config['resubmit_failed']
        self._autoterminate = autoterminate if autoterminate is not None else config['autoterminate']
        self._write_workflow = write_workflow if write_workflow is not None else config['write_workflow']
        self._rts = rts if rts in ['radical.pilot', 'mock'] else str(config['rts'])
        self._rmq_cleanup = rmq_cleanup if rmq_cleanup is not None else config['rmq_cleanup']
        self._rts_config = rts_config if rts_config is not None else config['rts_config']

        self._num_pending_qs = config['pending_qs']
        self._num_completed_qs = config['completed_qs']

    # ------------------------------------------------------------------------------------------------------------------
    # Getter functions
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def name(self):
        """
        Name for the application manager. Allows the user to setup the name of
        the application manager, as well as, its session ID. This name should be
        unique between different EnTK executions, otherwise it will produce an
        error.

        :getter: Returns the name of the application manager
        :setter: Assigns the name of the application manager
        :type: String
        """

        return self._name

    @property
    def sid(self):
        """
        Get the session ID of the current EnTK execution

        :getter: Returns the session ID of the EnTK execution
        :type: String
        """

        return self._sid

    @property
    def resource_desc(self):
        """
        :getter: Returns the resource description
        :setter: Assigns a resource description
        """

        return self._resource_desc

    @property
    def workflow(self):
        """
        :getter: Return the workflow assigned for execution
        :setter: Assign workflow to be executed
        """

        return self._workflow

    @property
    def shared_data(self):
        """
        :getter: Return list of filenames that are shared between multiple tasks of the application
        :setter: Assign a list of names of files that need to be staged to the remote machine
        """

        return self._shared_data

    # ------------------------------------------------------------------------------------------------------------------
    # Setter functions
    # ------------------------------------------------------------------------------------------------------------------

    @name.setter
    def name(self, value):

        if not isinstance(value, str):
            raise TypeError(expected_type=str, actual_type=type(value))

        else:
            self._name = value

    @resource_desc.setter
    def resource_desc(self, value):

        if self._rts == 'radical.pilot':
            from radical.entk.execman.rp import ResourceManager
            self._resource_manager = ResourceManager(resource_desc=value,
                                                     sid=self._sid,
                                                     rts_config=self._rts_config)
        elif self._rts == 'mock':
            from radical.entk.execman.mock import ResourceManager
            self._resource_manager = ResourceManager(resource_desc=value,
                                                     sid=self._sid)

        self._report.info('Validating and assigning resource manager')

        if self._resource_manager._validate_resource_desc():
            self._resource_manager._populate()
            self._resource_manager.shared_data = self._shared_data
        else:
            self._logger.error('Could not validate resource description')
            raise
        self._report.ok('>>ok\n')

    @workflow.setter
    def workflow(self, workflow):

        self._prof.prof('assigning workflow', uid=self._uid)

        for p in workflow:
            if not isinstance(p, Pipeline):
                self._logger.info('workflow type incorrect')
                raise TypeError(expected_type=['Pipeline', 'set of Pipelines'], actual_type=type(p))

            p._validate()

        self._workflow = workflow
        self._logger.info('Workflow assigned to Application Manager')

    @shared_data.setter
    def shared_data(self, data):

        if not isinstance(data, list):
            data = [data]

        for val in data:
            if not isinstance(val, str):
                raise TypeError(expected_type=str, actual_type=type(val))

        if self._resource_manager:
            self._resource_manager.shared_data = data


    # ------------------------------------------------------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------------------------------------------------------

    def run(self):
        """
        **Purpose**: Run the application manager. Once the workflow and resource manager have been assigned. Invoking this
        method will start the setting up the communication infrastructure, submitting a resource request and then
        submission of all the tasks.
        """

        try:

            # Set None objects local to each run
            self._wfp = None
            self._sync_thread = None
            self._terminate_sync = Event()
            self._resubmit_failed = False
            self._cur_attempt = 1

            if not self._workflow:
                self._logger.error('No workflow assigned currently, please check your script')
                raise MissingError(obj=self._uid, missing_attribute='workflow')

            if not self._resource_manager:
                self._logger.error(
                    'No resource manager assigned currently, please create and add a valid resource manager')
                raise MissingError(obj=self._uid, missing_attribute='resource_manager')

            self._prof.prof('amgr run started', uid=self._uid)

            # Setup rabbitmq stuff
            if not self._mqs_setup:

                self._report.info('Setting up RabbitMQ system')
                setup = self._setup_mqs()

                if not setup:
                    self._logger.error('RabbitMQ system not available')
                    raise EnTKError("RabbitMQ setup failed")

                self._mqs_setup = True

                self._report.ok('>>ok\n')

            # Create WFProcessor object
            self._prof.prof('creating wfp obj', uid=self._uid)
            self._wfp = WFprocessor(sid=self._sid,
                                    workflow=self._workflow,
                                    pending_queue=self._pending_queue,
                                    completed_queue=self._completed_queue,
                                    mq_hostname=self._mq_hostname,
                                    port=self._port,
                                    resubmit_failed=self._resubmit_failed)
            self._wfp._initialize_workflow()
            self._workflow = self._wfp.workflow


            # Submit resource request if not resource allocation done till now or
            # resubmit a new one if the old one has completed
            if self._resource_manager:
                res_alloc_state = self._resource_manager.get_resource_allocation_state()
                if (not res_alloc_state) or (res_alloc_state in self._resource_manager.get_completed_states()):

                    self._logger.info('Starting resource request submission')
                    self._prof.prof('init rreq submission', uid=self._uid)
                    self._resource_manager._submit_resource_request()

            else:

                self._logger.error(
                    'Cannot run without resource manager, please create and assign a resource manager')
                raise EnTKError(text='Missing resource manager')

            # Start synchronizer thread
            if not self._sync_thread:
                self._logger.info('Starting synchronizer thread')
                self._sync_thread = Thread(target=self._synchronizer, name='synchronizer-thread')
                self._prof.prof('starting synchronizer thread', uid=self._uid)
                self._sync_thread.start()

            # Start WFprocessor
            self._logger.info('Starting WFProcessor process from AppManager')
            self._wfp.start_processor()

            self._report.ok('All components created\n')

            # Create tmgr object only if it does not already exist
            if self._rts == 'radical.pilot':
                from radical.entk.execman.rp import TaskManager
            elif self._rts == 'mock':
                from radical.entk.execman.mock import TaskManager

            if not self._task_manager:
                self._prof.prof('creating tmgr obj', uid=self._uid)
                self._task_manager = TaskManager(sid=self._sid,
                                                 pending_queue=self._pending_queue,
                                                 completed_queue=self._completed_queue,
                                                 mq_hostname=self._mq_hostname,
                                                 rmgr=self._resource_manager,
                                                 port=self._port
                                                 )
                self._logger.info('Starting task manager process from AppManager')
                self._task_manager.start_manager()
                self._task_manager.start_heartbeat()

            active_pipe_count = len(self._workflow)
            finished_pipe_uids = []

            # We wait till all pipelines of the workflow are marked
            # complete
            while ((active_pipe_count > 0) and
                    (self._wfp.workflow_incomplete()) and
                    (self._resource_manager.get_resource_allocation_state() not
                     in self._resource_manager.get_completed_states())):

                if active_pipe_count > 0:

                    for pipe in self._workflow:

                        with pipe.lock:

                            if (pipe.completed) and (pipe.uid not in finished_pipe_uids):

                                self._logger.info('Pipe %s completed' % pipe.uid)
                                finished_pipe_uids.append(pipe.uid)
                                active_pipe_count -= 1
                                self._logger.info('Active pipes: %s' % active_pipe_count)

                if (not self._sync_thread.is_alive()) and (self._cur_attempt <= self._reattempts):

                    self._sync_thread = Thread(target=self._synchronizer,
                                               name='synchronizer-thread')
                    self._logger.info('Restarting synchronizer thread')
                    self._prof.prof('restarting synchronizer', uid=self._uid)
                    self._sync_thread.start()

                    self._cur_attempt += 1

                if (not self._wfp.check_processor()) and (self._cur_attempt <= self._reattempts):

                    """
                    If WFP dies, both child threads are also cleaned out.
                    We simply recreate the wfp object with a copy of the workflow
                    in the appmanager and start the processor.
                    """

                    self._prof.prof('recreating wfp obj', uid=self._uid)
                    self._wfp = WFProcessor(
                        sid=self._sid,
                        workflow=self._workflow,
                        pending_queue=self._pending_queue,
                        completed_queue=self._completed_queue,
                        mq_hostname=self._mq_hostname,
                        port=self._port,
                        resubmit_failed=self._resubmit_failed)

                    self._logger.info('Restarting WFProcessor process from AppManager')
                    self._wfp.start_processor()

                    self._cur_attempt += 1

                if (not self._task_manager.check_heartbeat()) and (self._cur_attempt <= self._reattempts):

                    """
                    If the tmgr process or heartbeat dies, we simply start a
                    new process using the start_manager method. We do not
                    need to create a new instance of the TaskManager object
                    itself. We stop and start a new instance of the
                    heartbeat thread as well.
                    """
                    self._prof.prof('restarting tmgr process and heartbeat', uid=self._uid)

                    self._logger.info('Terminating heartbeat thread')
                    self._task_manager.terminate_heartbeat()
                    self._logger.info('Terminating tmgr process')
                    self._task_manager.terminate_manager()
                    self._logger.info('Restarting task manager process')
                    self._task_manager.start_manager()
                    self._logger.info('Restarting heartbeat thread')
                    self._task_manager.start_heartbeat()

                    self._cur_attempt += 1

            self._prof.prof('start termination', uid=self._uid)

            # Terminate threads in following order: wfp, helper, synchronizer
            self._logger.info('Terminating WFprocessor')
            self._wfp.terminate_processor()

            self._logger.info('Terminating synchronizer thread')
            self._terminate_sync.set()
            self._sync_thread.join()
            self._logger.info('Synchronizer thread terminated')

            if self._autoterminate:
                self.resource_terminate()

            if self._write_workflow:
                write_workflow(self._workflow, self._sid)

            self._prof.prof('termination done', uid=self._uid)

        except KeyboardInterrupt:

            self._prof.prof('start termination', uid=self._uid)

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                               'trying to cancel enqueuer thread gracefully...')

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

            if self._resource_manager:
                self._resource_manager._terminate_resource_request()

            self._prof.prof('termination done', uid=self._uid)

            raise KeyboardInterrupt

        except Exception, ex:

            self._prof.prof('start termination', uid=self._uid)

            self._logger.exception('Error in AppManager: %s' % ex)

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

            if self._resource_manager:
                self._resource_manager._terminate_resource_request()

            self._prof.prof('termination done', uid=self._uid)
            raise

    def resource_terminate(self):

        if self._task_manager:
            self._logger.info('Terminating task manager process')
            self._task_manager.terminate_manager()
            self._task_manager.terminate_heartbeat()

        if self._resource_manager:
            self._resource_manager._terminate_resource_request()

        if os.environ.get('RADICAL_ENTK_PROFILE', False):
            write_session_description(self)

        if self._rmq_cleanup:
            self._cleanup_mqs()

        self._report.info('All components terminated\n')

    # ------------------------------------------------------------------------------------------------------------------
    # Private methods
    # ------------------------------------------------------------------------------------------------------------------

    def _setup_mqs(self):
        """
        **Purpose**: Setup RabbitMQ system on the client side. We instantiate queue(s) 'pendingq-*' for communication
        between the enqueuer thread and the task manager process. We instantiate queue(s) 'completedq-*' for
        communication between the task manager and dequeuer thread. We instantiate queue 'sync-to-master' for
        communication from enqueuer/dequeuer/task_manager to the synchronizer thread. We instantiate queue
        'sync-ack' for communication from synchronizer thread to enqueuer/dequeuer/task_manager.

        Details: All queues are durable: Even if the RabbitMQ server goes down, the queues are saved to disk and can
        be retrieved. This also means that after an erroneous run the queues might still have unacknowledged messages
        and will contain messages from that run. Hence, in every new run, we first delete the queue and create a new
        one.
        """

        try:

            self._prof.prof('init mqs setup', uid=self._uid)

            self._logger.debug('Setting up mq connection and channel')

            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname, port=self._port))

            mq_channel = mq_connection.channel()
            self._logger.debug('Connection and channel setup successful')
            self._logger.debug('Setting up all exchanges and queues')

            qs = [
                '%s-tmgr-to-sync' % self._sid,
                '%s-cb-to-sync' % self._sid,
                '%s-enq-to-sync' % self._sid,
                '%s-deq-to-sync' % self._sid,
                '%s-sync-to-tmgr' % self._sid,
                '%s-sync-to-cb' % self._sid,
                '%s-sync-to-enq' % self._sid,
                '%s-sync-to-deq' % self._sid
            ]

            for i in range(1, self._num_pending_qs + 1):
                queue_name = '%s-pendingq-%s' % (self._sid, i)
                self._pending_queue.append(queue_name)
                qs.append(queue_name)

            for i in range(1, self._num_completed_qs + 1):
                queue_name = '%s-completedq-%s' % (self._sid, i)
                self._completed_queue.append(queue_name)
                qs.append(queue_name)

            f = open('.%s.txt' % self._sid, 'w')
            for q in qs:
                # Durable Qs will not be lost if rabbitmq server crashes
                mq_channel.queue_declare(queue=q)
                f.write(q + '\n')
            f.close()

            self._logger.debug('All exchanges and queues are setup')
            self._prof.prof('mqs setup done', uid=self._uid)

            return True

        except Exception, ex:

            self._logger.error('Error setting RabbitMQ system: %s' % ex)
            raise

    def _cleanup_mqs(self):

        try:

            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname, port=self._port))
            mq_channel = mq_connection.channel()

            mq_channel.queue_delete(queue='%s-tmgr-to-sync' % self._sid)
            mq_channel.queue_delete(queue='%s-cb-to-sync' % self._sid)
            mq_channel.queue_delete(queue='%s-enq-to-sync' % self._sid)
            mq_channel.queue_delete(queue='%s-deq-to-sync' % self._sid)
            mq_channel.queue_delete(queue='%s-sync-to-tmgr' % self._sid)
            mq_channel.queue_delete(queue='%s-sync-to-cb' % self._sid)
            mq_channel.queue_delete(queue='%s-sync-to-enq' % self._sid)
            mq_channel.queue_delete(queue='%s-sync-to-deq' % self._sid)

            for i in range(1, self._num_pending_qs + 1):
                queue_name = '%s-pendingq-%s' % (self._sid, i)
                mq_channel.queue_delete(queue=queue_name)

            for i in range(1, self._num_completed_qs + 1):
                queue_name = '%s-completedq-%s' % (self._sid, i)
                mq_channel.queue_delete(queue=queue_name)

        except Exception as ex:
            self._logger.exception('Message queues not deleted, error: %s' % ex)
            raise

    def _synchronizer(self):
        """
        **Purpose**: Thread in the master process to keep the workflow data
        structure in appmanager up to date. We receive pipelines, stages and
        tasks objects directly. The respective object is updated in this master
        process.

        Details: Important to note that acknowledgements of the type
        channel.basic_ack() is an acknowledgement to the server that the msg
        was received. This is not to be confused with the Ack sent to the
        enqueuer/dequeuer/task_manager through the sync-ack queue.
        """

        try:

            self._prof.prof('synchronizer started', uid=self._uid)

            self._logger.info('synchronizer thread started')

            def task_update(msg, reply_to, corr_id, mq_channel):

                completed_task = Task()
                completed_task.from_dict(msg['object'])
                self._logger.info('Received %s with state %s' % (completed_task.uid, completed_task.state))

                found_task = False

                # Traverse the entire workflow to find the correct task
                for pipe in self._workflow:

                    if not pipe.completed:
                        if completed_task.parent_pipeline['uid'] == pipe.uid:

                            for stage in pipe.stages:

                                if completed_task.parent_stage['uid'] == stage.uid:

                                    for task in stage.tasks:

                                        if (completed_task.uid == task.uid)and(completed_task.state != task.state):

                                            task.state = str(completed_task.state)
                                            self._logger.debug('Found task %s with state %s' %
                                                               (task.uid, task.state))

                                            if completed_task.path:
                                                task.path = str(completed_task.path)

                                            mq_channel.basic_publish(exchange='',
                                                                     routing_key=reply_to,
                                                                     properties=pika.BasicProperties(
                                                                         correlation_id=corr_id),
                                                                     body='%s-ack' % task.uid)

                                            self._prof.prof('publishing sync ack for obj with state %s' %
                                                            msg['object']['state'],
                                                            uid=msg['object']['uid']
                                                            )

                                            mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                                            self._report.ok('Update: ')
                                            self._report.info('Task %s in state %s\n' % (task.uid, task.state))

                                            found_task = True

                                    if not found_task:

                                        # If there was a Stage update, but the Stage was not found in any of the Pipelines. This
                                        # means that this was a Stage that was added during runtime and the AppManager does not
                                        # know about it. The current solution is going to be: add it to the workflow object in the
                                        # AppManager via the synchronizer.

                                        self._prof.prof('Adap: adding new task')

                                        self._logger.info('Adding new task %s to parent stage: %s' % (completed_task.uid,
                                                                                                      stage.uid))

                                        stage.add_tasks(completed_task)
                                        mq_channel.basic_publish(exchange='',
                                                                 routing_key=reply_to,
                                                                 properties=pika.BasicProperties(
                                                                     correlation_id=corr_id),
                                                                 body='%s-ack' % completed_task.uid)

                                        self._prof.prof('Adap: added new task')

                                        self._prof.prof('publishing sync ack for obj with state %s' %
                                                        msg['object']['state'],
                                                        uid=msg['object']['uid']
                                                        )

                                        mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                                        self._report.ok('Update: ')
                                        self._report.info('Task %s in state %s\n' %
                                                          (completed_task.uid, completed_task.state))

            def stage_update(msg, reply_to, corr_id, mq_channel):

                completed_stage = Stage()
                completed_stage.from_dict(msg['object'])
                self._logger.info('Received %s with state %s' % (completed_stage.uid, completed_stage.state))

                found_stage = False

                # Traverse the entire workflow to find the correct stage
                for pipe in self._workflow:

                    if not pipe.completed:

                        if completed_stage.parent_pipeline['uid'] == pipe.uid:
                            self._logger.info('Found parent pipeline: %s' % pipe.uid)

                            for stage in pipe.stages:

                                if (completed_stage.uid == stage.uid)and(completed_stage.state != stage.state):

                                    self._logger.debug('Found stage %s' % stage.uid)

                                    stage.state = str(completed_stage.state)

                                    mq_channel.basic_publish(exchange='',
                                                             routing_key=reply_to,
                                                             properties=pika.BasicProperties(
                                                                 correlation_id=corr_id),
                                                             body='%s-ack' % stage.uid)

                                    self._prof.prof('publishing sync ack for obj with state %s' %
                                                    msg['object']['state'],
                                                    uid=msg['object']['uid']
                                                    )

                                    mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                                    self._report.ok('Update: ')
                                    self._report.info('Stage %s in state %s\n' % (stage.uid, stage.state))

                                    found_stage = True

                            if not found_stage:

                                # If there was a Stage update, but the Stage was not found in any of the Pipelines. This
                                # means that this was a Stage that was added during runtime and the AppManager does not
                                # know about it. The current solution is going to be: add it to the workflow object in the
                                # AppManager via the synchronizer.

                                self._prof.prof('Adap: adding new stage', uid=self._uid)

                                self._logger.info('Adding new stage %s to parent pipeline: %s' % (completed_stage.uid,
                                                                                                  pipe.uid))

                                pipe.add_stages(completed_stage)
                                mq_channel.basic_publish(exchange='',
                                                         routing_key=reply_to,
                                                         properties=pika.BasicProperties(
                                                             correlation_id=corr_id),
                                                         body='%s-ack' % completed_stage.uid)

                                self._prof.prof('Adap: adding new stage', uid=self._uid)

                                self._prof.prof('publishing sync ack for obj with state %s' %
                                                msg['object']['state'],
                                                uid=msg['object']['uid']
                                                )

                                mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            def pipeline_update(msg, reply_to, corr_id, mq_channel):

                completed_pipeline = Pipeline()
                completed_pipeline.from_dict(msg['object'])

                self._logger.info('Received %s with state %s' % (completed_pipeline.uid, completed_pipeline.state))

                # Traverse the entire workflow to find the correct pipeline
                for pipe in self._workflow:

                    if not pipe.completed:

                        if (completed_pipeline.uid == pipe.uid)and(completed_pipeline.state != pipe.state):

                            pipe.state = str(completed_pipeline.state)

                            self._logger.info('Found pipeline %s, state %s, completed %s' % (pipe.uid,
                                                                                             pipe.state,
                                                                                             pipe.completed)
                                              )

                            # Reply with ack msg to the sender
                            mq_channel.basic_publish(exchange='',
                                                     routing_key=reply_to,
                                                     properties=pika.BasicProperties(
                                                         correlation_id=corr_id),
                                                     body='%s-ack' % pipe.uid)

                            self._prof.prof('publishing sync ack for obj with state %s' %
                                            msg['object']['state'],
                                            uid=msg['object']['uid']
                                            )

                            mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                            # Keep the assignment of the completed flag after sending the acknowledgment
                            # back. Otherwise the MainThread takes lock over the pipeline because of logging
                            # and profiling
                            if completed_pipeline.completed:
                                pipe._completed_flag.set()
                            self._report.ok('Update: ')
                            self._report.info('Pipeline %s in state %s\n' % (pipe.uid, pipe.state))

            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname, port=self._port))
            mq_channel = mq_connection.channel()

            last = time.time()

            while not self._terminate_sync.is_set():

                #-------------------------------------------------------------------------------------------------------
                # Messages between tmgr Main thread and synchronizer -- only Task objects

                method_frame, props, body = mq_channel.basic_get(queue='%s-tmgr-to-sync' % self._sid)

                """
                The message received is a JSON object with the following structure:

                msg = {
                        'type': 'Pipeline'/'Stage'/'Task',
                        'object': json/dict
                        }
                """

                if body:

                    msg = json.loads(body)

                    self._prof.prof('received obj with state %s for sync' %
                                    msg['object']['state'], uid=msg['object']['uid'])

                    self._logger.debug('received %s with state %s for sync' %
                                       (msg['object']['uid'], msg['object']['state']))

                    if msg['type'] == 'Task':
                        task_update(msg, '%s-sync-to-tmgr' % self._sid, props.correlation_id, mq_channel)

                #-------------------------------------------------------------------------------------------------------

                #-------------------------------------------------------------------------------------------------------
                # Messages between callback thread and synchronizer -- only Task objects

                method_frame, props, body = mq_channel.basic_get(queue='%s-cb-to-sync' % self._sid)

                """
                The message received is a JSON object with the following structure:

                msg = {
                        'type': 'Pipeline'/'Stage'/'Task',
                        'object': json/dict
                        }
                """

                if body:

                    msg = json.loads(body)

                    self._prof.prof('received obj with state %s for sync' %
                                    msg['object']['state'], uid=msg['object']['uid'])

                    self._logger.debug('received %s with state %s for sync' %
                                       (msg['object']['uid'], msg['object']['state']))

                    if msg['type'] == 'Task':
                        task_update(msg, '%s-sync-to-cb' % self._sid, props.correlation_id, mq_channel)

                #-------------------------------------------------------------------------------------------------------

                #-------------------------------------------------------------------------------------------------------
                # Messages between enqueue thread and synchronizer -- Task, Stage or Pipeline
                method_frame, props, body = mq_channel.basic_get(queue='%s-enq-to-sync' % self._sid)

                if body:

                    msg = json.loads(body)

                    self._prof.prof('received obj with state %s for sync' %
                                    msg['object']['state'], uid=msg['object']['uid'])

                    self._logger.debug('received %s with state %s for sync' %
                                       (msg['object']['uid'], msg['object']['state']))

                    if msg['type'] == 'Task':
                        task_update(msg, '%s-sync-to-enq' % self._sid, props.correlation_id, mq_channel)

                    elif msg['type'] == 'Stage':
                        stage_update(msg, '%s-sync-to-enq' % self._sid, props.correlation_id, mq_channel)

                    elif msg['type'] == 'Pipeline':
                        pipeline_update(msg, '%s-sync-to-enq' % self._sid, props.correlation_id, mq_channel)
                #-------------------------------------------------------------------------------------------------------

                #-------------------------------------------------------------------------------------------------------
                # Messages between dequeue thread and synchronizer -- Task, Stage or Pipeline
                method_frame, props, body = mq_channel.basic_get(queue='%s-deq-to-sync' % self._sid)

                if body:

                    msg = json.loads(body)

                    self._prof.prof('received obj with state %s for sync' %
                                    msg['object']['state'], uid=msg['object']['uid'])

                    self._logger.debug('received %s with state %s for sync' %
                                       (msg['object']['uid'], msg['object']['state']))

                    if msg['type'] == 'Task':
                        task_update(msg, '%s-sync-to-deq' % self._sid, props.correlation_id, mq_channel)

                    elif msg['type'] == 'Stage':
                        stage_update(msg, '%s-sync-to-deq' % self._sid, props.correlation_id, mq_channel)

                    elif msg['type'] == 'Pipeline':
                        pipeline_update(msg, '%s-sync-to-deq' % self._sid, props.correlation_id, mq_channel)
                #-------------------------------------------------------------------------------------------------------

                # Appease pika cos it thinks the connection is dead
                now = time.time()
                if now - last >= self._rmq_ping_interval:
                    mq_connection.process_data_events()
                    last = now

            self._prof.prof('terminating synchronizer', uid=self._uid)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                               'trying to terminate synchronizer thread gracefully...')

            raise KeyboardInterrupt

        except Exception, ex:

            self._logger.exception('Unknown error in synchronizer: %s. \n Terminating thread' % ex)
            raise

    # ------------------------------------------------------------------------------------------------------------------
