__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__ = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__ = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
from multiprocessing import Process, Event
from radical.entk import states, Pipeline, Task
from radical.entk.utils.init_transition import transition
import time
from time import sleep
import json
import threading
import pika
import traceback
import os
import uuid


class WFprocessor(object):

    """
    An WFProcessor (workflow processor) takes the responsibility of dispatching tasks from the various pipelines of the
    workflow according to their relative order to the TaskManager. All state updates are communicated to the AppManager.

    :Arguments:
        :workflow: COPY of the entire workflow existing in the AppManager 
        :pending_qs: number of queues to hold pending tasks
        :completed_qs: number of queues to hold completed tasks
        :mq_hostname: hostname where the RabbitMQ is live
        :port: port at which rabbitmq can be accessed
    """

    def __init__(self,
                 sid,
                 workflow,
                 pending_queue,
                 completed_queue,
                 mq_hostname,
                 port,
                 resubmit_failed):


        if isinstance(sid, str):
            self._sid = sid
        else:
            raise TypeError(expected_type=str, actual_type=type(sid))

        self._uid = ru.generate_id('radical.entk.wfprocessor.%(item_counter)04d', ru.ID_CUSTOM, namespace=self._sid)
        self._path = os.getcwd() + '/' + self._sid

        self._logger = ru.get_logger(self._uid, path=self._path)
        self._prof = ru.Profiler(name=self._uid + '-obj', path=self._path)

        self._prof.prof('create wfp obj', uid=self._uid)

        self._workflow = workflow
        self._validate_workflow()


        if not isinstance(pending_queue, list):
            raise TypeError(expected_type=list, actual_type=type(pending_queue))
        self._pending_queue = pending_queue

        if not isinstance(completed_queue, list):
            raise TypeError(expected_type=list, actual_type=type(completed_queue))
        self._completed_queue = completed_queue

        if not isinstance(mq_hostname, str) and not isinstance(mq_hostname, unicode):
            raise TypeError(expected_type=str, actual_type=type(mq_hostname))
        self._mq_hostname = mq_hostname

        if not isinstance(port, int):
            raise TypeError(expected_type=int, actual_type=type(port))
        self._port = port

        if not isinstance(resubmit_failed, bool):
            raise TypeError(expected_type=bool, actual_type=type(resubmit_failed))
        self._resubmit_failed = resubmit_failed

        self._wfp_process = None
        
        self._logger.info('Created WFProcessor object: %s' % self._uid)

        self._prof.prof('wfp obj created', uid=self._uid)

        
    # ------------------------------------------------------------------------------------------------------------------
    # Getter
    # ------------------------------------------------------------------------------------------------------------------
    @property
    def workflow(self):
        return self._workflow

    # ------------------------------------------------------------------------------------------------------------------
    # Private Methods
    # ------------------------------------------------------------------------------------------------------------------

    def _validate_workflow(self):
        """
        **Purpose**: Validate whether the workflow consists of a set of Pipelines and validate each Pipeline. 

        Details: Tasks are validated when being added to Stage. Stages are validated when being added to Pipelines. Only
        Pipelines themselves remain to be validated before execution.
        """

        try:

            self._prof.prof('validating workflow', uid=self._uid)

            if not isinstance(self._workflow, set):

                if not isinstance(self._workflow, list):
                    self._workflow = set([self._workflow])
                else:
                    self._workflow = set(self._workflow)

            for item in self._workflow:
                if not isinstance(item, Pipeline):
                    self._logger.info('workflow type incorrect')
                    raise TypeError(expected_type=['Pipeline', 'set of Pipeline'],
                                    actual_type=type(item))

                item._validate()

            self._prof.prof('workflow validated', uid=self._uid)

        except Exception, ex:

            self._logger.error('Fatal error while adding workflow to appmanager: %s' % ex)
            raise

    #

    def _wfp(self):
        """
        **Purpose**: This is the function executed in the wfp process. The function is used to simply create
        and spawn two threads: enqueue, dequeue. The enqueue thread pushes ready tasks to the queues in the pending_q slow
        list whereas the dequeue thread pulls completed tasks from the queues in the completed_q. This function is also
        responsible for the termination of these threads and hence blocking.
        """

        try:

            local_prof = ru.Profiler(name=self._uid + '-proc', path=self._path)

            local_prof.prof('wfp process started', uid=self._uid)

            self._logger.info('WFprocessor started')

            # Process should run till terminate condtion is encountered
            while (not self._wfp_terminate.is_set()):

                try:

                    # Start dequeue thread
                    if (not self._dequeue_thread) or (not self._dequeue_thread.is_alive()):

                        local_prof.prof('creating dequeue-thread', uid=self._uid)
                        self._dequeue_thread = threading.Thread(
                            target=self._dequeue, args=(local_prof,), name='dequeue-thread')

                        self._logger.info('Starting dequeue-thread')
                        local_prof.prof('starting dequeue-thread', uid=self._uid)
                        self._dequeue_thread.start()

                    # Start enqueue thread
                    if (not self._enqueue_thread) or (not self._enqueue_thread.is_alive()):

                        local_prof.prof('creating enqueue-thread', uid=self._uid)
                        self._enqueue_thread = threading.Thread(
                            target=self._enqueue, args=(local_prof,), name='enqueue-thread')

                        self._logger.info('Starting enqueue-thread')
                        local_prof.prof('starting enqueue-thread', uid=self._uid)
                        self._enqueue_thread.start()

                except Exception, ex:
                    self._logger.error('WFProcessor interrupted')
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

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                               'trying to cancel wfprocessor process gracefully...')

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

            raise KeyboardInterrupt

        except Exception, ex:
            self._logger.error('Error in wfp process: %s. \n Closing enqueue, dequeue threads' % ex)

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

            print traceback.format_exc()
            raise Error(text=ex)

    def _enqueue(self, local_prof):
        """
        **Purpose**: This is the function that is run in the enqueue thread. This function extracts Tasks from the 
        copy of workflow that exists in the WFprocessor object and pushes them to the queues in the pending_q list.
        Since this thread works on the copy of the workflow, every state update to the Task, Stage and Pipeline is
        communicated back to the AppManager (master process) via the 'sync_with_master' function that has dedicated
        queues to communicate with the master.

        Details: Termination condition of this thread is set by the wfp process.
        """

        try:

            local_prof.prof('enqueue-thread started', uid=self._uid)
            self._logger.info('enqueue-thread started')

            if os.environ.get('DISABLE_RMQ_HEARTBEAT', None):
                self._mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname,
                                                                                        port=self._port,
                                                                                        heartbeat=0
                                                                                        )
                                                              )
            else:
                self._mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname,
                                                                                        port=self._port
                                                                                        )
                                                              )
            mq_channel = self._mq_connection.channel()

            while not self._enqueue_thread_terminate.is_set():

                for pipe in self._workflow:

                    with pipe._stage_lock:

                        if not pipe.completed:

                            # Test if the pipeline is already in the final state
                            if pipe.state in states.FINAL:
                                continue

                            elif pipe.state == states.INITIAL:

                                # Set state of pipeline to SCHEDULING if it is in INITIAL
                                transition(obj=pipe,
                                           obj_type='Pipeline',
                                           new_state=states.SCHEDULING,
                                           channel=mq_channel,
                                           queue='%s-enq-to-sync'%self._sid,
                                           profiler=local_prof,
                                           logger=self._logger)

                            if pipe.stages[pipe.current_stage - 1].state in [states.INITIAL, states.SCHEDULED]:


                                try:

                                    # Starting scheduling of tasks of current stage, so set state of stage to
                                    # SCHEDULING
                                    executable_stage = pipe.stages[pipe.current_stage - 1]

                                    if executable_stage.state == states.INITIAL:

                                        transition(obj=executable_stage,
                                                   obj_type='Stage',
                                                   new_state=states.SCHEDULING,
                                                   channel=mq_channel,
                                                   queue='%s-enq-to-sync'%self._sid,
                                                   profiler=local_prof,
                                                   logger=self._logger)

                                    executable_tasks = executable_stage.tasks

                                    for executable_task in executable_tasks:

                                        if self._resubmit_failed:

                                            if executable_task.state in [states.INITIAL, states.FAILED]:

                                                # Set state of Tasks in current Stage to SCHEDULING
                                                transition(obj=executable_task,
                                                           obj_type='Task',
                                                           new_state=states.SCHEDULING,
                                                           channel=mq_channel,
                                                           queue='%s-enq-to-sync'%self._sid,
                                                           profiler=local_prof,
                                                           logger=self._logger)

                                                task_as_dict = json.dumps(executable_task.to_dict())

                                                self._logger.debug('Publishing task %s to %s'
                                                                   % (executable_task.uid,
                                                                      self._pending_queue[0]))

                                                # Put the task on one of the pending_queues
                                                mq_channel.basic_publish(exchange='',
                                                                         routing_key=self._pending_queue[0],
                                                                         body=task_as_dict
                                                                         # properties=pika.BasicProperties(
                                                                         # make message persistent
                                                                         # delivery_mode = 2)
                                                                         )

                                                # Set state of Tasks in current Stage to SCHEDULED
                                                transition(obj=executable_task,
                                                           obj_type='Task',
                                                           new_state=states.SCHEDULED,
                                                           channel=mq_channel,
                                                           queue='%s-enq-to-sync'%self._sid,
                                                           profiler=local_prof,
                                                           logger=self._logger)

                                                tasks_submitted = True
                                                self._logger.debug('Task %s published to queue' % executable_task.uid)

                                        if executable_task.state == states.INITIAL:

                                            # Set state of Tasks in current Stage to SCHEDULING
                                            transition(obj=executable_task,
                                                       obj_type='Task',
                                                       new_state=states.SCHEDULING,
                                                       channel=mq_channel,
                                                       queue='%s-enq-to-sync'%self._sid,
                                                       profiler=local_prof,
                                                       logger=self._logger)

                                            task_as_dict = json.dumps(executable_task.to_dict())

                                            self._logger.debug('Publishing task %s to %s'
                                                               % (executable_task.uid,
                                                                  self._pending_queue[0]))

                                            # Put the task on one of the pending_queues
                                            mq_channel.basic_publish(exchange='',
                                                                     routing_key=self._pending_queue[0],
                                                                     body=task_as_dict
                                                                     # properties=pika.BasicProperties(
                                                                     # make message persistent
                                                                     # delivery_mode = 2)
                                                                     )

                                            # Set state of Tasks in current Stage to SCHEDULED
                                            transition(obj=executable_task,
                                                       obj_type='Task',
                                                       new_state=states.SCHEDULED,
                                                       channel=mq_channel,
                                                       queue='%s-enq-to-sync'%self._sid,
                                                       profiler=local_prof,
                                                       logger=self._logger)

                                            self._logger.debug('Task %s published to queue' % executable_task.uid)

                                    if executable_stage.state == states.SCHEDULING:
                                        # All tasks of current stage scheduled, so set state of stage to
                                        # SCHEDULED
                                        transition(obj=executable_stage,
                                                   obj_type='Stage',
                                                   new_state=states.SCHEDULED,
                                                   channel=mq_channel,
                                                   queue='%s-enq-to-sync'%self._sid,
                                                   profiler=local_prof,
                                                   logger=self._logger)
                                except Exception, ex:

                                    # If there is an error, explicitly switching the state of Stage to INITIAL
                                    self._logger.error('Error while updating stage ' +
                                                       'state, rolling back. Error: %s' % ex)

                                    executable_stage = pipe.stages[pipe.current_stage - 1]

                                    transition(obj=executable_stage,
                                               obj_type='Stage',
                                               new_state=states.INITIAL,
                                               channel=mq_channel,
                                               queue='%s-enq-to-sync'%self._sid,
                                               profiler=local_prof,
                                               logger=self._logger)

                                    raise

            self._logger.info('Enqueue thread terminated')
            self._mq_connection.close()

            local_prof.prof('terminating enqueue-thread', uid=self._uid)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                               'trying to cancel enqueuer thread gracefully...')

            self._mq_connection.close()

            raise KeyboardInterrupt

        except Exception, ex:

            self._logger.error('Error in enqueue-thread: %s' % ex)
            print traceback.format_exc()

            raise Error(text=ex)

    def _dequeue(self, local_prof):
        """
        **Purpose**: This is the function that is run in the dequeue thread. This function extracts Tasks from the 
        completed queus and updates the copy of workflow that exists in the WFprocessor object.
        Since this thread works on the copy of the workflow, every state update to the Task, Stage and Pipeline is
        communicated back to the AppManager (master process) via the 'sync_with_master' function that has dedicated
        queues to communicate with the master.

        Details: Termination condition of this thread is set by the wfp process.
        """

        try:

            local_prof.prof('dequeue-thread started', uid=self._uid)
            self._logger.info('Dequeue thread started')

            if os.environ.get('DISABLE_RMQ_HEARTBEAT', None):
                self._mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname,
                                                                                        port=self._port,
                                                                                        heartbeat=0
                                                                                        )
                                                              )
            else:
                self._mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname,
                                                                                        port=self._port
                                                                                        )
                                                              )
            mq_channel = self._mq_connection.channel()

            while not self._dequeue_thread_terminate.is_set():

                try:

                    method_frame, header_frame, body = mq_channel.basic_get(queue=self._completed_queue[0])

                    if body:

                        # Get task from the message
                        completed_task = Task(duplicate=True)
                        completed_task.from_dict(json.loads(body))
                        self._logger.info('Got finished task %s from queue' % (completed_task.uid))

                        transition(obj=completed_task,
                                   obj_type='Task',
                                   new_state=states.DEQUEUEING,
                                   channel=mq_channel,
                                   queue='%s-deq-to-sync'%self._sid,
                                   profiler=local_prof,
                                   logger=self._logger)

                        # Traverse the entire workflow to find out the correct Task
                        for pipe in self._workflow:

                            with pipe._stage_lock:

                                if not pipe.completed:

                                    if completed_task.parent_pipeline == pipe.uid:

                                        self._logger.debug('Found parent pipeline: %s' % pipe.uid)

                                        for stage in pipe.stages:

                                            if completed_task.parent_stage == stage.uid:
                                                self._logger.debug('Found parent stage: %s' % (stage.uid))

                                                transition(obj=completed_task,
                                                           obj_type='Task',
                                                           new_state=states.DEQUEUED,
                                                           channel=mq_channel,
                                                           queue='%s-deq-to-sync'%self._sid,
                                                           profiler=local_prof,
                                                           logger=self._logger)

                                                if completed_task.exit_code:
                                                    completed_task.state = states.FAILED
                                                else:
                                                    completed_task.state = states.DONE

                                                for task in stage.tasks:

                                                    if task.uid == completed_task.uid:
                                                        task.state = str(completed_task.state)

                                                        if (task.state == states.FAILED) and (self._resubmit_failed):
                                                            task.state = states.INITIAL

                                                        transition(obj=task,
                                                                   obj_type='Task',
                                                                   new_state=task.state,
                                                                   channel=mq_channel,
                                                                   queue='%s-deq-to-sync'%self._sid,
                                                                   profiler=local_prof,
                                                                   logger=self._logger)

                                                        if stage._check_stage_complete():

                                                            transition(obj=stage,
                                                                       obj_type='Stage',
                                                                       new_state=states.DONE,
                                                                       channel=mq_channel,
                                                                       queue='%s-deq-to-sync'%self._sid,
                                                                       profiler=local_prof,
                                                                       logger=self._logger)

                                                            pipe._increment_stage()

                                                            if pipe.completed:

                                                                transition(obj=pipe,
                                                                           obj_type='Pipeline',
                                                                           new_state=states.DONE,
                                                                           channel=mq_channel,
                                                                           queue='%s-deq-to-sync'%self._sid,
                                                                           profiler=local_prof,
                                                                           logger=self._logger)

                                                        # Found the task and processed it -- no more iterations needed

                                                        break

                                                # Found the stage and processed it -- no more iterations neeeded
                                                break

                                        # Found the pipeline and processed it -- no more iterations neeeded
                                        break

                        mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                except Exception, ex:
                    self._logger.error('Unable to receive message from completed queue: %s' % ex)
                    raise

            self._logger.info('Terminated dequeue thread')
            self._mq_connection.close()

            local_prof.prof('terminating dequeue-thread', uid=self._uid)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                               'trying to exit gracefully...')

            self._mq_connection.close()

            raise KeyboardInterrupt

        except Exception, ex:
            self._logger.error('Error in dequeue-thread: %s' % ex)
            print traceback.format_exc()

            self._mq_connection.close()

            raise Error(text=ex)

    # ------------------------------------------------------------------------------------------------------------------
    # Public Methods
    # ------------------------------------------------------------------------------------------------------------------

    def start_processor(self):
        """
        **Purpose**: Method to start the wfp process. The wfp function
        is not to be accessed directly. The function is started in a separate
        process using this method.
        """

        if not self._wfp_process:

            try:

                self._prof.prof('creating wfp process', uid=self._uid)
                self._wfp_process = Process(target=self._wfp, name='wfprocessor')

                self._enqueue_thread = None
                self._dequeue_thread = None
                self._enqueue_thread_terminate = threading.Event()
                self._dequeue_thread_terminate = threading.Event()

                self._wfp_terminate = Event()
                self._logger.info('Starting WFprocessor process')
                self._prof.prof('starting wfp process', uid=self._uid)
                self._wfp_process.start()

                return True

            except Exception, ex:

                self._logger.error('WFprocessor not started')
                self.end_processor()
                raise

        else:
            self._logger.warn('Wfp process already running, attempted to restart!')

    def end_processor(self):
        """
        **Purpose**: Method to terminate the wfp process. This method is 
        blocking as it waits for the wfp process to terminate (aka join).
        """

        try:

            self._logger.debug('Attempting to end WFprocessor... event: %s' % self._wfp_terminate.is_set())

            if self.check_alive():
                self._wfp_terminate.set()
                self._wfp_process.join()
                self._logger.debug('WFprocessor process terminated')
            else:
                self._logger.debug('WFprocessor process already terminated')

            self._prof.prof('wfp process terminated', uid=self._uid)

            self._prof.close()

        except Exception, ex:
            self._logger.error('Could not terminate wfprocessor process')
            raise

    def workflow_incomplete(self):
        """
        **Purpose**: Method to check if the workflow execution is incomplete.
        """

        try:
            for pipe in self._workflow:
                with pipe._stage_lock:
                    if pipe.completed:
                        pass
                    else:
                        return True
            return False

        except Exception, ex:
            self._logger.error('Could not check if workflow is incomplete, error:%s' % ex)
            raise

    def check_alive(self):
        """
        **Purpose**: Method to check if the wfp process is alive
        """

        return self._wfp_process.is_alive()

