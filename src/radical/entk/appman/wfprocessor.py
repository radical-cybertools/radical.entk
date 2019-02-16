
__copyright__ = "Copyright 2017-2019, http://radical.rutgers.edu"
__author__    = "RADICAL Team <radical@rutgers.edu>"
__license__   = "MIT"

import time
import json
import threading
import pika
import os

from multiprocessing import Process, Event

import radical.utils as ru

from ..exceptions import *
from .. import states, Task
from ..utils.init_transition import transition


# ------------------------------------------------------------------------------
#
class WFprocessor(object):

    """
    An WFprocessor (workflow processor) takes the responsibility of dispatching
    tasks from the various pipelines of the workflow according to their relative
    order to the TaskManager. All state updates are communicated to the
    AppManager.  The WFprocessor also retrieves completed tasks from the
    TaskManager and updates states of PST accordingly.

    :Arguments:
        :sid:             (str)  session id (used by profiler and loggers)
        :workflow:        (set)  COPY of the AppManager's workflow structure
        :pending_queue:   (list) queues to hold pending tasks
        :completed_queue: (list) queues to hold completed tasks
        :mq_hostname:     (str)  hostname where the RabbitMQ is alive
        :port:            (int)  port at which RabbitMQ can be accessed
        :resubmit_failed: (bool) True if failed tasks should be resubmitted
    """

    def __init__(self,
                 sid,
                 workflow,
                 pending_queue,
                 completed_queue,
                 mq_hostname,
                 port,
                 resubmit_failed):

        # Mandatory arguments
        self._sid = sid
        self._pending_queue = pending_queue
        self._completed_queue = completed_queue
        self._mq_hostname = mq_hostname
        self._port = port
        self._resubmit_failed = resubmit_failed

        # Assign validated workflow
        self._workflow = workflow

        # Create logger and profiler at their specific locations using the sid
        self._uid = ru.generate_id('wfprocessor.%(item_counter)04d',
                                   ru.ID_CUSTOM, namespace=self._sid)
        self._path = os.getcwd() + '/' + self._sid
        self._logger = ru.Logger('radical.entk.%s' % self._uid,
                                 path=self._path, targets=['2', '.'])
        self._prof = ru.Profiler(name='radical.entk.%s' % self._uid + '-obj',
                                 path=self._path)

        self._prof.prof('create wfp obj', uid=self._uid)

        # Defaults
        self._wfp_process = None
        self._rmq_ping_interval = os.getenv('RMQ_PING_INTERVAL', 10)

        self._logger.info('Created WFProcessor object: %s' % self._uid)
        self._prof.prof('wfp obj created', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    # Getter
    #
    @property
    def workflow(self):
        return self._workflow


    # --------------------------------------------------------------------------
    #
    # Private Methods
    #
    def _initialize_workflow(self):
        """
        **Purpose**: Initialize the PST of the workflow with uid and type checks
        """

        try:
            self._prof.prof('initializing workflow', uid=self._uid)

            for p in self._workflow:
                p._assign_uid(self._sid)

            self._prof.prof('workflow initialized', uid=self._uid)

        except Exception:
            self._logger.exception('Fatal error while initializing workflow')
            raise


    # --------------------------------------------------------------------------
    #
    def _enqueue(self, local_prof):
        """
        **Purpose**: This is the function that is run in the enqueue thread.
        This function extracts Tasks from the copy of workflow that exists in
        the WFprocessor object and pushes them to the queues in the pending_q
        list.  Since this thread works on the copy of the workflow, every state
        update to the Task, Stage and Pipeline is communicated back to the
        AppManager (master process) via the 'sync_with_master' function that has
        dedicated queues to communicate with the master.

        Details: Termination condition of this thread is set by the wfp process.
        """

        try:
            local_prof.prof('enqueue-thread started', uid=self._uid)
            self._logger.info('enqueue-thread started')

            # Acquire a connection+channel to the rmq server
            mq_connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self._mq_hostname, port=self._port))
            mq_channel = mq_connection.channel()

            last = time.time()
            while not self._enqueue_thread_terminate.is_set():

                # We iterate through all pipelines to collect tasks from stages
                # that are pending scheduling. Once collected, these tasks will
                # be communicated to the tmgr in bulk.

                workload = []
                scheduled_stages = []

                for pipe in self._workflow:

                    with pipe.lock:

                        # Test if the pipeline is already in the final state
                        if  pipe.state in states.FINAL    or \
                            pipe.completed                or \
                            pipe.state == states.SUSPENDED:
                            continue

                        if pipe.state == states.INITIAL:

                            # Set state of pipeline to SCHEDULING if it is in INITIAL
                            transition(obj=pipe,
                                       obj_type='Pipeline',
                                       new_state=states.SCHEDULING,
                                       channel=mq_channel,
                                       queue='%s-enq-to-sync' % self._sid,
                                       profiler=local_prof,
                                       logger=self._logger)

                        executable_stage = pipe.stages[pipe.current_stage - 1]

                        if not executable_stage.uid:
                            # AM: why are pipe UID's assigned here, but not via
                            # assign_uid?
                            executable_stage.parent_pipeline['uid']  = pipe.uid
                            executable_stage.parent_pipeline['name'] = pipe.name
                            executable_stage._assign_uid(self._sid)


                        if executable_stage.state in [states.INITIAL,
                                                      states.SCHEDULED]:

                            if executable_stage.state == states.INITIAL:

                                transition(obj=executable_stage,
                                           obj_type='Stage',
                                           new_state=states.SCHEDULING,
                                           channel=mq_channel,
                                           queue='%s-enq-to-sync' % self._sid,
                                           profiler=local_prof,
                                           logger=self._logger)

                            executable_tasks = executable_stage.tasks

                            for executable_task in executable_tasks:

                                if executable_task.state not in [states.INITIAL,
                                                                 states.FAILED]:
                                    continue

                                if  executable_task.state == states.FAILED and \
                                    not self._resubmit_failed:
                                    continue

                                # Set task states in current Stage to SCHEDULING
                                transition(obj=executable_task,
                                           obj_type='Task',
                                           new_state=states.SCHEDULING,
                                           channel=mq_channel,
                                           queue='%s-enq-to-sync' % self._sid,
                                           profiler=local_prof,
                                           logger=self._logger)

                                workload.append(executable_task)

                                # I don't understand the flow.  Why is the
                                # stage appended for each task in that
                                # stage?
                                #
                                # for task in exe_stage.tasks:
                                #   if exe_stage not in sched_stages:
                                #       sched_stages.append(exe_stage)
                                # 
                                # that append can happen only once, right?
                                if executable_stage not in scheduled_stages:
                                    scheduled_stages.append(executable_stage)

                if workload:

                    # Put the task on one of the pending_queues
                    # AM'  WL_as_*dict* is a *list*? ;-)
                    workload_as_dict = [task.to_dict() for task in workload]

                    mq_channel.basic_publish(exchange='',
                                             routing_key=self._pending_queue[0],
                                             body=json.dumps(workload_as_dict)
                                             # properties=pika.BasicProperties(
                                             # make message persistent
                                             # delivery_mode = 2)
                                             )

                for task in workload:

                    # Set state of Tasks in current Stage to SCHEDULED
                    transition(obj=task,
                               obj_type='Task',
                               new_state=states.SCHEDULED,
                               channel=mq_channel,
                               queue='%s-enq-to-sync' % self._sid,
                               profiler=local_prof,
                               logger=self._logger)

                    self._logger.debug(
                        'Task %s published to pending queue' % task.uid)


                for executable_stage in scheduled_stages:

                    transition(obj=executable_stage,
                               obj_type='Stage',
                               new_state=states.SCHEDULED,
                               channel=mq_channel,
                               queue='%s-enq-to-sync' % self._sid,
                               profiler=local_prof,
                               logger=self._logger)

                # Appease pika cos it thinks the connection is dead
                now = time.time()
                if now - last >= self._rmq_ping_interval:
                    mq_connection.process_data_events()
                    last = now

            self._logger.info('Enqueue thread terminated')
            mq_connection.close()

            local_prof.prof('terminating enqueue-thread', uid=self._uid)


        except KeyboardInterrupt:

            self._logger.exception('Execution interrupted by user ' +
                                   '(you probably hit Ctrl+C), trying ' +
                                   'to cancel enqueuer thread gracefully...')
            mq_connection.close()
            raise


        except Exception:

            self._logger.exception('Error in enqueue-thread')
            try:
                mq_connection.close()

            except Exception as ex:
                # AM: close failed, but messages warns about not created?
                self._logger.warning('mq_connection not created, %s' % ex)

            raise


    # --------------------------------------------------------------------------
    #
    def _dequeue(self, local_prof):
        """
        **Purpose**: This is the function that is run in the dequeue thread.
        This function extracts Tasks from the completed queus and updates the
        copy of workflow that exists in the WFprocessor object.  Since this
        thread works on the copy of the workflow, every state update to the
        Task, Stage and Pipeline is communicated back to the AppManager (master
        process) via the 'sync_with_master' function that has dedicated queues
        to communicate with the master.

        Details: Termination condition of this thread is set by the wfp process.
        """

        try:
            local_prof.prof('dequeue-thread started', uid=self._uid)
            self._logger.info('Dequeue thread started')

            mq_connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self._mq_hostname, port=self._port))
            mq_channel = mq_connection.channel()

            last = time.time()

            while not self._dequeue_thread_terminate.is_set():

                try:
                    method_frame, header_frame, body = mq_channel.basic_get(
                        queue=self._completed_queue[0])

                    if body:
                        # Get task from the message
                        completed_task = Task()
                        completed_task.from_dict(json.loads(body))
                        self._logger.info(
                            'Got finished task %s from queue' % (completed_task.uid))

                        transition(obj=completed_task,
                                   obj_type='Task',
                                   new_state=states.DEQUEUEING,
                                   channel=mq_channel,
                                   queue='%s-deq-to-sync' % self._sid,
                                   profiler=local_prof,
                                   logger=self._logger)

                        # Traverse the entire workflow to find out the correct Task
                        for pipe in self._workflow:

                            with pipe.lock:

                                if ((not pipe.completed) and (not pipe.state == states.SUSPENDED)):

                                    if completed_task.parent_pipeline['uid'] == pipe.uid:

                                        self._logger.debug(
                                            'Found parent pipeline: %s' % pipe.uid)

                                        for stage in pipe.stages:

                                            if completed_task.parent_stage['uid'] == stage.uid:
                                                self._logger.debug(
                                                    'Found parent stage: %s' % (stage.uid))

                                                transition(obj=completed_task,
                                                           obj_type='Task',
                                                           new_state=states.DEQUEUED,
                                                           channel=mq_channel,
                                                           queue='%s-deq-to-sync' % self._sid,
                                                           profiler=local_prof,
                                                           logger=self._logger)

                                                if not completed_task.exit_code:
                                                    completed_task.state = states.DONE
                                                else:
                                                    completed_task.state = states.FAILED

                                                for task in stage.tasks:

                                                    if task.uid == completed_task.uid:
                                                        task.state = str(
                                                            completed_task.state)

                                                        if (task.state == states.FAILED) and (self._resubmit_failed):
                                                            task.state = states.INITIAL

                                                        transition(obj=task,
                                                                   obj_type='Task',
                                                                   new_state=task.state,
                                                                   channel=mq_channel,
                                                                   queue='%s-deq-to-sync' % self._sid,
                                                                   profiler=local_prof,
                                                                   logger=self._logger)

                                                        if stage._check_stage_complete():

                                                            transition(obj=stage,
                                                                       obj_type='Stage',
                                                                       new_state=states.DONE,
                                                                       channel=mq_channel,
                                                                       queue='%s-deq-to-sync' % self._sid,
                                                                       profiler=local_prof,
                                                                       logger=self._logger)

                                                            # Check if Stage has a post-exec that needs to be
                                                            # executed

                                                            if stage.post_exec['condition']:

                                                                try:

                                                                    self._logger.info(
                                                                        'Executing post-exec for stage %s' % stage.uid)
                                                                    self._prof.prof('Adap: executing post-exec',
                                                                                    uid=self._uid)

                                                                    func_condition = stage.post_exec['condition']
                                                                    func_on_true = stage.post_exec['on_true']
                                                                    func_on_false = stage.post_exec['on_false']

                                                                    if func_condition():
                                                                        func_on_true()
                                                                    else:
                                                                        func_on_false()

                                                                    self._logger.info(
                                                                        'Post-exec executed for stage %s' % stage.uid)
                                                                    self._prof.prof(
                                                                        'Adap: post-exec executed', uid=self._uid)

                                                                except Exception, ex:
                                                                    self._logger.exception(
                                                                        'Execution failed in post_exec of stage %s' % stage.uid)
                                                                    raise

                                                            pipe._increment_stage()

                                                            if pipe.completed:

# AM: This is indentatin level 16.  Sorry, but I kind of refuse to try to read
# that 200-line loop.  This needs to be simplifies.
#
# Most of your nestings are:
#
#   for x in y:
#     if x.state == something:
#        z = x.get_z
#        for a in z:
#          if a.state == something:
#            bs = a.get_b
#
#   and at (in)finitium.  You have a PST model - the nesting should be at most
#   3 levels (+try/excepts etc).  Please replace search loops with filters
#
#      zs = [x.get_z for x in y if x.state == something]
#
#   Also, turn positive if's into negative ones:
#
#      for x in y:
#        if x.state = s1:
#           z = x.get_z
#           for a in z:
#               ...
#
#   should be:
#
#      for x in y:
#
#        # only want final X
#        if x.state != FINAL:
#           continue
#
#        z = x.get_z
#        for a in z:
#            ...
#
#   That removes the if-conditions from the nesting depths

                                                                transition(obj=pipe,
                                                                           obj_type='Pipeline',
                                                                           new_state=states.DONE,
                                                                           channel=mq_channel,
                                                                           queue='%s-deq-to-sync' % self._sid,
                                                                           profiler=local_prof,
                                                                           logger=self._logger)

                                                        # Found the task and processed it -- no more iterations needed

                                                        break

                                                # Found the stage and processed it -- no more iterations neeeded
                                                break

                                        # Found the pipeline and processed it -- no more iterations neeeded
                                        break

                        mq_channel.basic_ack(
                            delivery_tag=method_frame.delivery_tag)

                    # Appease pika cos it thinks the connection is dead
                    now = time.time()
                    if now - last >= self._rmq_ping_interval:
                        mq_connection.process_data_events()
                        last = now

                except Exception, ex:
                    self._logger.exception(
                        'Unable to receive message from completed queue: %s' % ex)
                    raise

            self._logger.info('Terminated dequeue thread')
            mq_connection.close()

            local_prof.prof('terminating dequeue-thread', uid=self._uid)


        except KeyboardInterrupt:

            self._logger.exception('Execution interrupted by user (you probably'
                                   'hit Ctrl+C), trying to exit gracefully...')
            mq_connection.close()
            raise


        except Exception as ex:

            self._logger.exception('Error in dequeue-thread')
            try:
                mq_connection.close()
            except:
                self._logger.warning('mq_connection not created')

            raise EnTKError(ex)


    # --------------------------------------------------------------------------
    #
    def _wfp(self):
        """
        **Purpose**: This is the function executed in the wfp process. The
        function is used to simply create and spawn two threads: enqueue,
        dequeue. The enqueue thread pushes ready tasks to the queues in the
        pending_q slow list whereas the dequeue thread pulls completed tasks
        from the queues in the completed_q. This function is also responsible
        for the termination of these threads and hence blocking.
        """

        try:
            # AM FIXME: create Profiler in thread
            local_prof = ru.Profiler(name='radical.entk.%s' % self._uid + '-proc',
                                     path=self._path)

            local_prof.prof('wfp process started', uid=self._uid)
            self._logger.info('WFprocessor started')

            # Process should run till terminate condtion is encountered
            while not self._wfp_terminate.is_set():

                try:
                    # Start dequeue thread
                    if  not self._dequeue_thread or \
                        not self._dequeue_thread.is_alive():

                        local_prof.prof('creating dequeue-thread', uid=self._uid)
                        self._dequeue_thread = threading.Thread(
                                target=self._dequeue, args=(local_prof,),
                                name='dequeue-thread')

                        self._logger.info('Starting dequeue-thread')
                        local_prof.prof('starting dequeue-thread', uid=self._uid)
                        self._dequeue_thread.start()

                    # Start enqueue thread
                    if  not self._enqueue_thread or \
                        not self._enqueue_thread.is_alive():

                        local_prof.prof('creating enqueue-thread', uid=self._uid)
                        self._enqueue_thread = threading.Thread(
                                target=self._enqueue, args=(local_prof,),
                                name='enqueue-thread')

                        self._logger.info('Starting enqueue-thread')
                        local_prof.prof('starting enqueue-thread', uid=self._uid)
                        self._enqueue_thread.start()

                except Exception:
                    self._logger.exception('WFProcessor interrupted')
                    raise


            local_prof.prof('start termination', uid=self._uid)

            self._logger.info('Terminating enqueue-thread')
            self._enqueue_thread_terminate.set()
            self._enqueue_thread.join()
            self._logger.info('Terminating dequeue-thread')
            self._dequeue_thread_terminate.set()
            self._dequeue_thread.join()

            local_prof.prof('termination done', uid=self._uid)
            local_prof.prof('terminating wfp process', uid=self._uid)
            local_prof.close()

        except KeyboardInterrupt:
            self._logger.exception('Execution interrupted by user (you probably'
                                   'hit Ctrl+C), rying to cancel wfprocessor'
                                   'process gracefully...')
            self._terminate_threads()
            raise


        except Exception as ex:
            self._logger.exception('Error in wfp process - closing enqueue,'
                                   'dequeue threads')
            self._terminate_threads()

            self._logger.exception('%s failed with %s' % (self._uid, ex))
            raise EnTKError(ex)


    # --------------------------------------------------------------------------
    #
    def _terminate_threads(self):

        if self._enqueue_thread:
            if not self._enqueue_thread_terminate.is_set():
                self._logger.info('Terminating enqueue-thread')
                self._enqueue_thread_terminate.set()
                self._enqueue_thread.join()

        if self._dequeue_thread:
            if not self._dequeue_thread_terminate.is_set():
                self._logger.info('Terminating dequeue-thread')
                self._dequeue_thread_terminate.set()
                self._dequeue_thread.join()

        self._logger.info('WFprocessor process terminated')


    # --------------------------------------------------------------------------
    #
    # Public Methods
    #
    def start_processor(self):
        """
        **Purpose**: Method to start the wfp process. The wfp function
        is not to be accessed directly. The function is started in a separate
        process using this method.
        """

        if not self._wfp_process:

            try:

                self._prof.prof('creating wfp process', uid=self._uid)
                self._wfp_process = Process(
                    target=self._wfp, name='wfprocessor')

                self._enqueue_thread = None
                self._dequeue_thread = None
                self._enqueue_thread_terminate = threading.Event()
                self._dequeue_thread_terminate = threading.Event()

                self._wfp_terminate = Event()
                self._logger.info('Starting WFprocessor process')
                self._prof.prof('starting wfp process', uid=self._uid)
                self._wfp_process.start()

                return True

            except Exception:

                self._logger.exception('WFprocessor not started')
                self.terminate_processor()
                raise

        else:
            self._logger.warn('Wfp process already running, attempted to restart!')


    # --------------------------------------------------------------------------
    #
    def terminate_processor(self):
        """
        **Purpose**: Method to terminate the wfp process. This method is
        blocking as it waits for the wfp process to terminate (aka join).
        """

        try:

            if self.check_processor():
                self._logger.debug('Attempting to end WFprocessor... event: %s'
                                  % self._wfp_terminate.is_set())
                self._wfp_terminate.set()
                self._wfp_process.join()
                self._wfp_process = None
                self._logger.debug('WFprocessor process terminated')

            else:
                self._logger.debug('WFprocessor process already terminated')

            self._prof.prof('wfp process terminated', uid=self._uid)
            self._prof.close()

        except Exception:
            self._logger.exception('Could not terminate wfprocessor process')
            raise


    # --------------------------------------------------------------------------
    #
    def workflow_incomplete(self):
        """
        **Purpose**: Method to check if the workflow execution is incomplete.
        """

        try:
            for pipe in self._workflow:
                with pipe.lock:
                    if not pipe.completed:
                        return True
            return False

        except Exception:
            self._logger.exception('Could not check if workflow is incomplete')
            raise


    # --------------------------------------------------------------------------
    #
    def check_processor(self):
        """
        **Purpose**: Method to check if the wfp process is alive
        """

        if self._wfp_process:
            return self._wfp_process.is_alive()


# ------------------------------------------------------------------------------

