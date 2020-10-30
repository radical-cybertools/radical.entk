
__copyright__ = "Copyright 2017-2019, http://radical.rutgers.edu"
__author__    = "RADICAL Team <radical@rutgers.edu>"
__license__   = "MIT"


import os
import json
import pika
import time
import threading

import radical.utils as ru

# EnTK imports
from .. import states, Task


# ------------------------------------------------------------------------------
#
class WFprocessor(object):
    """
    An WFprocessor (workflow processor) takes the responsibility of dispatching
    tasks from the various pipelines of the workflow according to their relative
    order to the TaskManager. All state updates are relflected in the AppManager
    as we operate on the reference of the same workflow object. The WFprocessor
    also retrieves completed tasks from the TaskManager and updates states of
    PST accordingly.

    :Arguments:
        :sid:             (str) session id used by the profiler and loggers
        :workflow:        (set) REFERENCE of the AppManager's workflow
        :pending_queue:   (list) queues to hold pending tasks
        :completed_queue: (list) queues to hold completed tasks
        :resubmit_failed: (bool) True if failed tasks should be resubmitted
        :rmq_conn_params: (pika.connection.ConnectionParameters) object of
                          parameters necessary to connect to RabbitMQ
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self,
                 sid,
                 workflow,
                 pending_queue,
                 completed_queue,
                 resubmit_failed,
                 rmq_conn_params):

        # Mandatory arguments
        self._sid             = sid
        self._pending_queue   = pending_queue
        self._completed_queue = completed_queue
        self._resubmit_failed = resubmit_failed
        self._rmq_conn_params = rmq_conn_params

        # Assign validated workflow
        self._workflow = workflow

        # Create logger and profiler at their specific locations using the sid
        self._path = os.getcwd() + '/' + self._sid
        self._uid  = ru.generate_id('wfprocessor.%(counter)04d', ru.ID_CUSTOM)

        name = 'radical.entk.%s' % self._uid
        self._logger = ru.Logger  (name, path=self._path)
        self._prof   = ru.Profiler(name, path=self._path)
        self._report = ru.Reporter(name)

        # Defaults
        self._wfp_process       = None
        self._enqueue_thread    = None
        self._dequeue_thread    = None
        self._enqueue_thread_terminate = None
        self._dequeue_thread_terminate = None
        self._rmq_ping_interval = int(os.getenv('RMQ_PING_INTERVAL', '10'))

        self._logger.info('Created WFProcessor object: %s' % self._uid)
        self._prof.prof('create_wfp', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _advance(self, obj, obj_type, new_state):
        '''
        transition `obj` of type `obj_type` into state `new_state`
        '''

        # NOTE: this is a local operation, no queue communication is involved
        #       (other than the `_advance()` in the TaskManager classes, which

        if   obj_type == 'Task' : msg = obj.parent_stage['uid']
        elif obj_type == 'Stage': msg = obj.parent_pipeline['uid']
        else                    : msg = None

        obj.state = new_state

        self._prof.prof('advance', uid=obj.uid, state=obj.state, msg=msg)
        self._report.ok('Update: ')
        self._report.info('%s state: %s\n' % (obj.luid, obj.state))
        self._logger.info('Transition %s to state %s' % (obj.uid, new_state))


    # --------------------------------------------------------------------------
    # Getter
    #
    @property
    def workflow(self):
        return self._workflow


    # --------------------------------------------------------------------------
    # Private Methods
    #
    def _create_workload(self):

        # We iterate through all pipelines to collect tasks from
        # stages that are pending scheduling. Once collected, these tasks
        # will be communicated to the tmgr in bulk.

        # Initial empty list to store executable tasks across different
        # pipelines
        workload = list()

        # The executable tasks can belong to different pipelines, and
        # hence different stages. Empty list to store the stages so that
        # we can update the state of stages accordingly
        scheduled_stages = list()

        for pipe in self._workflow:

            with pipe.lock:

                # If Pipeline is in the final state or suspended, we
                # skip processing it.
                if pipe.state in states.FINAL or  \
                    pipe.completed or \
                    pipe.state == states.SUSPENDED:

                    continue

                if pipe.state == states.INITIAL:

                    # Set state of pipeline to SCHEDULING if it is in INITIAL
                    self._advance(pipe, 'Pipeline', states.SCHEDULING)

                # Get the next stage of this pipeline to process
                exec_stage = pipe.stages[pipe.current_stage - 1]

                if not exec_stage.uid:
                    # TODO: Move parent uid, name assignment to assign_uid()
                    exec_stage.parent_pipeline['uid']  = pipe.uid
                    exec_stage.parent_pipeline['name'] = pipe.name

                # If its a new stage, update its state
                if exec_stage.state == states.INITIAL:

                    self._advance(exec_stage, 'Stage', states.SCHEDULING)

                # Get all tasks of a stage in SCHEDULED state
                exec_tasks = list()
                if exec_stage.state == states.SCHEDULING:
                    exec_tasks = exec_stage.tasks

                for exec_task in exec_tasks:

                    state = exec_task.state
                    if   state == states.INITIAL or \
                        (state == states.FAILED and self._resubmit_failed):

                        # Set state of Tasks in current Stage
                        # to SCHEDULING
                        self._advance(exec_task, 'Task', states.SCHEDULING)

                        # Store the tasks from different pipelines
                        # into our workload list. All tasks will
                        # be submitted in bulk and their states
                        # will be updated accordingly
                        workload.append(exec_task)

                        # We store the stages since the stages the
                        # above tasks belong to also need to be
                        # updated. If its a task that failed, the
                        # stage is already in the correct state
                        if exec_task.state == states.FAILED:
                            continue
                        if exec_stage not in scheduled_stages:
                            scheduled_stages.append(exec_stage)

        return workload, scheduled_stages


    # --------------------------------------------------------------------------
    #
    def _execute_workload(self, workload, scheduled_stages):

        # Tasks of the workload need to be converted into a dict
        # as pika can send and receive only json/dict data
        wl_json = json.dumps([task.to_dict() for task in workload])

        # Acquire a connection+channel to the rmq server
        mq_connection = pika.BlockingConnection(self._rmq_conn_params)
        mq_channel = mq_connection.channel()

        # Send the workload to the pending queue
        mq_channel.basic_publish(exchange = '',
                                    routing_key=self._pending_queue[0],
                                    body=wl_json

                                    # TODO: Make durability parameters
                                    # as a config parameter and then
                                    # enable the following accordingly
                                    # properties=pika.BasicProperties(
                                    # make message persistent
                                    # delivery_mode = 2)

                                    )
        self._logger.debug('Workload submitted to Task Manager')

        # Update the state of the tasks in the workload
        for task in workload:

            # Set state of Tasks in current Stage to SCHEDULED
            self._advance(task, 'Task', states.SCHEDULED)

        # Update the state of the stages from which tasks have
        # been scheduled
        if scheduled_stages:
            for executable_stage in scheduled_stages:
                self._advance(executable_stage, 'Stage', states.SCHEDULED)


    # --------------------------------------------------------------------------
    #
    def _enqueue(self):
        """
        **Purpose**: This is the function that is run in the enqueue thread.
        This function extracts Tasks from the workflow that exists in
        the WFprocessor object and pushes them to the queues in the pending_q
        list.
        """

        try:

            self._prof.prof('enq_start', uid=self._uid)
            self._logger.info('enqueue-thread started')

            while not self._enqueue_thread_terminate.is_set():

                time.sleep(3)

                workload, scheduled_stages = self._create_workload()

                # If there are tasks to be executed
                if workload:
                    self._execute_workload(workload, scheduled_stages)

            self._logger.info('Enqueue thread terminated')
            self._prof.prof('enq_stop', uid=self._uid)

        except KeyboardInterrupt:

            self._logger.exception('Execution interrupted by user (you \
                                    probably hit Ctrl+C), trying to cancel \
                                    enqueuer thread gracefully...')
            raise

        except Exception:

            self._logger.exception('Error in enqueue-thread')
            raise



    # --------------------------------------------------------------------------
    #
    def _update_dequeued_task(self, deq_task):

        # Traverse the entire workflow to find out the correct Task
        # TODO: Investigate whether we can change the DS of the
        # workflow so that we don't have this expensive search
        # for each task.
        # First search across all pipelines
        # Note: deq_task is not the same as the task that exists in this process,
        # they are different objects and have different state histories.
        for pipe in self._workflow:

            with pipe.lock:

                # Skip pipelines that have completed or are
                # currently suspended
                if pipe.completed or pipe.state == states.SUSPENDED:
                    continue

                # Skip pipelines that don't match the UID
                # There will be only one pipeline that matches
                if pipe.uid != deq_task.parent_pipeline['uid']:
                    continue

                self._logger.debug('Found parent pipeline: %s' %
                                    pipe.uid)

                # Next search across all stages of a matching
                # pipelines
                assert(pipe.stages)

                for stage in pipe.stages:

                    # Skip stages that don't match the UID
                    # There will be only one stage that matches
                    if stage.uid != deq_task.parent_stage['uid']:
                        continue

                    self._logger.debug('Found parent stage: %s' %
                                        stage.uid)

                    # Search across all tasks of matching stage
                    for task in stage.tasks:

                        # Skip tasks that don't match the UID
                        # There will be only one task that matches
                        if task.uid != deq_task.uid:
                            continue

                        # If there is no exit code, we assume success
                        # We are only concerned about state of task and not
                        # deq_task
                        if not deq_task.exit_code:
                            task_state = states.DONE
                        else:
                            task_state = states.FAILED

                        if task.state == states.FAILED and \
                            self._resubmit_failed:
                            task_state = states.INITIAL
                        self._advance(task, 'Task', task_state)

                        # Found the task and processed it -- no more
                        # iterations needed
                        break

                    # Found the stage and processed it -- no more
                    # iterations needed for the current task
                    break

                assert(stage)
                # Check if current stage has completed
                # If yes, we need to (i) check for post execs to
                # be executed and (ii) check if it is the last
                # stage of the pipeline -- update pipeline
                # state if yes.
                if stage._check_stage_complete():
                    self._advance(stage, 'Stage', states.DONE)

                    # Check if the current stage has a post-exec
                    # that needs to be executed
                    if stage.post_exec:
                        self._execute_post_exec(pipe, stage)

                    # if pipeline got suspended, advance state accordingly
                    if pipe.state == states.SUSPENDED:
                        self._advance(pipe, 'Pipeline', states.SUSPENDED)

                    else:
                        # otherwise perform normal stage progression
                        pipe._increment_stage()

                    # If pipeline has completed, advance state to DONE
                    if pipe.completed:
                        self._advance(pipe, 'Pipeline', states.DONE)


                # Found the pipeline and processed it -- no more
                # iterations needed for the current task
                break


    # --------------------------------------------------------------------------
    #
    def _execute_post_exec(self, pipe, stage):
        """
        **Purpose**: This method executes the post_exec step of a stage for a
        pipeline.
        """
        try:
            self._logger.info('Executing post-exec for stage %s' % stage.uid)
            self._prof.prof('post_exec_start', uid=self._uid)
            resumed_pipe_uids = stage.post_exec()
            self._logger.info('Post-exec executed for stage %s' % stage.uid)
            self._prof.prof('post_exec_stop', uid=self._uid)


        except Exception:
            self._logger.exception('post_exec of stage %s failed' % stage.uid)
            self._prof.prof('post_exec_fail', uid=self._uid)
            raise

        if resumed_pipe_uids:

            for r_pipe in self._workflow:

                if r_pipe == pipe:
                    continue

                with r_pipe.lock:
                    if r_pipe.uid in resumed_pipe_uids:

                        # Resumed pipelines already have the correct state,
                        # they just need to be synced with the AppMgr.
                        r_pipe._increment_stage()

                        if r_pipe.completed:
                            self._advance(r_pipe, 'Pipeline', states.DONE)

                        else:
                            self._advance(r_pipe, 'Pipeline', r_pipe.state)



    # --------------------------------------------------------------------------
    #
    def _dequeue(self):
        """
        **Purpose**: This is the function that is run in the dequeue thread.
        This function extracts Tasks from the completed queues and updates the
        workflow.
        """

        try:

            self._prof.prof('deq_start', uid=self._uid)
            self._logger.info('Dequeue thread started')

            # Acquire a connection+channel to the rmq server
            mq_connection = pika.BlockingConnection(self._rmq_conn_params)
            mq_channel = mq_connection.channel()

            last = time.time()
            while not self._dequeue_thread_terminate.is_set():

                method_frame, _ , body = mq_channel.basic_get(
                    queue=self._completed_queue[0])

                # When there is no msg received, body is None
                if not body:
                    continue

                # Acknowledge the received message
                mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                # Create a  task from the received msg
                deq_task = Task()
                deq_task.from_dict(json.loads(body))
                self._logger.info('Got finished task %s from queue'
                                  % (deq_task.uid))
                self._update_dequeued_task(deq_task)

                # Appease pika cos it thinks the connection is dead
                now = time.time()
                if now - last >= self._rmq_ping_interval:
                    mq_connection.process_data_events()
                    last = now

            self._logger.info('Terminated dequeue thread')
            self._prof.prof('deq_stop', uid=self._uid)


        except KeyboardInterrupt:
            self._logger.exception('Execution interrupted by user (you \
                                    probably hit Ctrl+C), trying to exit \
                                    gracefully...')
            raise


        except Exception:
            self._logger.exception('Error in dequeue-thread')
            raise


        finally:
            try:
                mq_connection.close()
            except Exception as ex:
                self._logger.warning('mq_connection close failed, %s' % ex)
            self._logger.debug('closed mq_connection')


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

        try:

            self._logger.info('Starting WFprocessor')
            self._prof.prof('wfp_start', uid=self._uid)

            self._enqueue_thread_terminate = threading.Event()
            self._dequeue_thread_terminate = threading.Event()

            # Start dequeue thread
            self._dequeue_thread = threading.Thread(target=self._dequeue,
                                                    name='dequeue-thread')
            self._logger.info('Starting dequeue-thread')
            self._prof.prof('starting dequeue-thread', uid=self._uid)
            self._dequeue_thread.start()

            # Start enqueue thread
            self._enqueue_thread = threading.Thread(target=self._enqueue,
                                                    name='enqueue-thread')
            self._logger.info('Starting enqueue-thread')
            self._prof.prof('starting enqueue-thread', uid=self._uid)
            self._enqueue_thread.start()

            self._logger.info('WFprocessor started')
            self._prof.prof('wfp_started', uid=self._uid)

        except:

            self._logger.exception('WFprocessor failed')
            self.terminate_processor()
            raise


    # --------------------------------------------------------------------------
    #
    def terminate_processor(self):
        """
        **Purpose**: Method to terminate the wfp process. This method is
        blocking as it waits for the wfp process to terminate (aka join).
        """

        try:

            if self._enqueue_thread:

                if not self._enqueue_thread_terminate.is_set():
                    self._logger.info('Terminating enqueue-thread')
                    self._enqueue_thread_terminate.set()
                    self._enqueue_thread.join()
                    self._enqueue_thread = None

            if self._dequeue_thread:

                if not self._dequeue_thread_terminate.is_set():
                    self._logger.info('Terminating dequeue-thread')
                    self._dequeue_thread_terminate.set()
                    self._dequeue_thread.join()
                    self._dequeue_thread = None

            self._logger.info('WFprocessor terminated')
            self._prof.prof('wfp_stop', uid=self._uid)
            self._prof.close()

        except:

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
                    if pipe.completed:
                        pass
                    else:
                        return True
            return False

        except Exception as ex:
            self._logger.exception(
                'Could not check if workflow is incomplete, error:%s' % ex)
            raise


    # --------------------------------------------------------------------------
    #
    def check_processor(self):

        if self._enqueue_thread is None or self._dequeue_thread is None:
            return False

        if not self._enqueue_thread.is_alive() or \
                not self._dequeue_thread.is_alive():
            return False

        return True


# ------------------------------------------------------------------------------

