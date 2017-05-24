__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk.pipeline.pipeline import Pipeline
from radical.entk.task.task import Task
from wfprocessor import WFProcessor
from helper import Helper
import sys, time
import Queue
import pika
import json
from threading import Thread, Event

class AppManager(object):

    def __init__(self, hostname = 'localhost', push_threads=1, pull_threads=1, 
                sync_threads=1, pending_qs=1, completed_qs=1):

        self._uid       = ru.generate_id('radical.entk.appmanager')
        self._name      = str()

        self._workload  = None
        self._resubmit_failed = False

        # RabbitMQ Queues
        self._num_pending_qs = pending_qs
        self._num_completed_qs = completed_qs
        self._pending_queue = list()
        self._completed_queue = list()
        # RabbitMQ inits
        self._mq_connection = None
        self._mq_channel = None
        self._mq_hostname = hostname


        # Threads and procs
        self._num_push_threads = push_threads
        self._num_pull_threads = pull_threads
        self._num_sync_threads = sync_threads
        self._wf_done = Event()
        
        # Logger
        self._logger = ru.get_logger('radical.entk.appmanager')
        self._logger.info('Application Manager initialized')

    # -----------------------------------------------
    # Getter functions
    # -----------------------------------------------

    @property
    def name(self):
        return self._name

    @property
    def resubmit_failed(self):
        return self._resubmit_failed
    
    # -----------------------------------------------
    # Setter functions
    # -----------------------------------------------

    @name.setter
    def name(self, value):
        self._name = value

    @resubmit_failed.setter
    def resubmit_failed(self, value):
        self._resubmit_failed = value


    # Function to add workload to the application manager
    # ------------------------------------------------------

    def validate_workload(self, workload):

        if not isinstance(workload, set):

            if not isinstance(workload, list):
                workload = set([workload])
            else:
                workload = set(workload)


        for item in workload:
            if not isinstance(item, Pipeline):
                self._logger.info('Workload type incorrect')
                raise TypeError(expected_type=['Pipeline', 'set of Pipeline'], 
                                actual_type=type(item))

        return workload


    def assign_workload(self, workload):

        try:
            
            self._workload = self.validate_workload(workload)
            self._logger.info('Workload assigned to Application Manager')

        except TypeError:
            raise

        except Exception, ex:

            self._logger.error('Fatal error while adding workload to appmanager')
            raise UnknownError(text=ex)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user ' + 
                        '(you probably hit Ctrl+C), tring to exit gracefully...')
            sys.exit(1)


    def setup_mqs(self):

        self._mq_connection = pika.BlockingConnection(
                                pika.ConnectionParameters(host=self._mq_hostname)
                                )
        self._mq_channel = connection.channel()

        self._mq_channel.exchange_declare(exchange='fork', type='fanout')

        for i in range(1,self._num_pending_qs+1):
            queue_name = 'pendingq-%s'%i
            self._pending_queue.append(queue_name)
            self._mq_channel.queue_declare(queue=queue_name)

        for i in range(1,self._num_completed_qs+1):
            queue_name = 'completedq-%s'%i
            self._completed_queue.append(queue_name)
            self._mq_channel.queue_declare(queue=queue_name)
            self._mq_channel.queue_bind(exchange='fork', queue=queue_name)

        self._mq_channel.queue_declare(queue='synchronizerq')
        self._mq_channel.queue_bind(exchange='fork', queue='synchronizerq')


    def synchronizer(self):

        while not self._wf_done.is_set():

            method_frame, header_frame, body = self._mq_channel.basic_get(
                                                        queue='synchronizerq'
                                                        )

            if body:

                completed_task = Task()
                completed_task.load_from_dict(json.loads(body))

                for pipe in self._workload:

                    if not pipe.completed:

                        if completed_task.parent_pipeline == pipe.uid:

                            self._logger.debug('Found parent pipeline: %s'%pipe.uid)
                            
                            for stage in pipe.stages:

                                if completed_task.parent_stage == stage.uid:
                                    self._logger.debug('Found parent stage: %s'%(stage.uid))

                                    #task.state = states.DONE
                                    self._logger.info('Task %s in Stage %s of Pipeline %s: %s'%(
                                                        task.uid,
                                                        pipe.stages[pipe.current_stage].uid,
                                                        pipe.uid,
                                                        task.state))

                                    for task in stage:

                                        if task.uid == completed_task.uid:
                                            task = completed_task

                                            if task.state == states.DONE:

                                                if stage.check_tasks_status():

                                                    try:
                                                        self._logger.info('All tasks of stage %s finished' %(stage.uid))
                                                        stage.state = states.DONE
                                                        pipe.increment_stage()

                                                    except Exception, ex:
                                                        # Rolling back stage status
                                                        self._logger.error('Error while updating stage '+
                                                                'state, rolling back. Error: %s'%ex)
                                                        stage.state = stage.SCHEDULED
                                                        pipe.decrement_stage()                                                    

                                                    if pipe.completed:
                                                        #self._workload.remove(pipe)
                                                        self._logger.info('Pipelines %s has completed'%(pipe.uid))
                                                        pipe.state = states.DONE

                                            elif completed_task.state == states.FAILED:

                                                if self._resubmit_failed:

                                                    try:
                                                        new_task = Task()
                                                        new_task.replicate(completed_task)

                                                        pipe.stages[pipe.current_stage].add_tasks(new_task)

                                                    except Exception, ex:
                                                        self._logger.error("Resubmission of task %s failed, error: %s"%
                                                                                                (completed_task.uid,ex))

                                                else:

                                                    if stage.check_tasks_status():

                                                        try:
                                                            self._logger.info('All tasks of stage %s finished' %
                                                                                                        (stage.uid))
                                                            stage.state = states.DONE
                                                            pipe.increment_stage()

                                                        except Exception, ex:
                                                            # Rolling back stage status
                                                            self._logger.error('Error while updating stage '+
                                                                    'state, rolling back. Error: %s'%ex)
                                                            stage.state = stage.SCHEDULED
                                                            pipe.decrement_stage()                                                    

                                                        if pipe.completed:
                                                            #self._workload.remove(pipe)
                                                            self._logger.info('Pipelines %s has completed'%(pipe.uid))
                                                            pipe.state = states.DONE

                                            else:

                                                # Task is canceled
                                                pass

                                            # Found the task and processed it -- no more iterations needed
                                            break

                                    # Found the stage and processed it -- no more iterations neeeded
                                    break

                            # Found the pipeline and processed it -- no more iterations neeeded
                            break

                self._mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)


    def run(self):

        try:

            if self._workload is None:
                print 'Assign workload before invoking run method - cannot proceed'
                self._logger.info('No workload assigned currently, please check your script')
                raise ValueError(expected_value='set of pipelines', actual_value=None)

            else:

                # Setup rabbitmq stuff
                self.setup_mqs()

                # Start synchronizer thread
                sync_thread = Thread(target=synchronizer, name='synchronizer-thread')
                sync_thread.start()


                # Create WFProcessor object
                wfp = WFProcessor(  workload = self._workload, 
                                    pending_queue = self._pending_queue, 
                                    completed_queue=self._completed_queue,
                                    channel=self._mq_channel)

                wfp.resubmit_failed = self._resubmit_failed
                wfp.start_processor()

                helper = Helper(    pending_queue = self._pending_queue, 
                                    completed_queue=self._completed_queue,
                                    channel=self._mq_channel)

                helper.start_helper()
                
                active_pipe_count = len(self._workload)
                while active_pipe_count > 0:
                    time.sleep(1)

                    for pipe in self._workload:
                        if pipe.completed:
                            active_pipe_count -= 1

                    if not wfp.check_alive():

                        wfp = WFProcessor(  workload = self._workload, 
                                            pending_queue = self._pending_queue, 
                                            completed_queue=self._completed_queue,
                                            channel=self._mq_channel)

                        wfp.resubmit_failed = self._resubmit_failed
                        wfp.start_processor()

                    if not helper.check_alive():
                        helper = Helper(    pending_queue = self._pending_queue, 
                                            executed_queue=self._executed_queue)
                        helper.start_helper()

                    if not sync_thread.check_alive():
                        sync_thread = Thread(   target=synchronizer, 
                                                name='synchronizer-thread')
                        sync_thread.start()



                # Terminate threads in following order: enqueuer, helper, dequeuer
                self._logger.info('Closing WFprocessor')
                wfp.terminate()
                self._logger.info('WFprocessor closed')                
                
                self._logger.info('Closing helper thread')
                helper.terminate()
                self._logger.info('Helper thread closed')

                self._logger.info('Closing synchronizer thread')
                self._wf_done.set()
                stnc_thread.join()
                self._logger.info('Synchronizer thread closed')


        except Exception, ex:

            self._logger.error('Fatal error while running appmanager')
            
            # Terminate threads in following order: enqueuer, helper, dequeuer
            self._logger.info('Closing enqueuer thread')
            enqueuer.terminate()
            self._logger.info('Enqueuer thread closed')                
            self._logger.info('Closing helper thread')
            helper.terminate()
            self._logger.info('Helper thread closed')
            self._logger.info('Closing dequeuer thread')
            dequeuer.terminate()
            self._logger.info('Dequeuer thread closed')

            raise UnknownError(text=ex)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                            'trying to exit gracefully...')

            # Terminate threads in following order: enqueuer, helper, dequeuer
            self._logger.info('Closing enqueuer thread')
            enqueuer.terminate()
            self._logger.info('Enqueuer thread closed')                
            self._logger.info('Closing helper thread')
            helper.terminate()
            self._logger.info('Helper thread closed')
            self._logger.info('Closing dequeuer thread')
            dequeuer.terminate()
            self._logger.info('Dequeuer thread closed')

            sys.exit(1)
