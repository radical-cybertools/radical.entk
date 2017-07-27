__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
from multiprocessing import Process, Event
import Queue
from radical.entk import states, Task
from radical.entk.utils.sync_initiator import sync_with_master
import time
import json
import pika
import traceback
import os
import radical.pilot as rp
from task_processor import create_cud_from_task, create_task_from_cu
import uuid

slow_run = os.environ.get('RADICAL_ENTK_SLOW',False)

class TaskManager(object):

    """
    A Task Manager takes the responsibility of dispatching tasks it receives from a queue for execution on to 
    the available resources using a runtime system. In this case, the runtime system being used RADICAL Pilot. Once 
    the tasks have completed execution, they are pushed on to another queue for other components of EnTK to access.


    :arguments:
        :pending_queue: List of queue(s) with tasks ready to be executed. Currently, only one queue.
        :completed_queue: List of queue(s) with tasks that have finished execution. Currently, only one queue.
        :mq_hostname: Name of the host where RabbitMQ is running
        :rmgr: ResourceManager object to be used to access the Pilot where the tasks can be submitted

    Currently, EnTK is configured to work with one pending queue and one completed queue. In the future, the number of 
    queues can be varied for different throughput requirements at the cost of additional Memory and CPU consumption.
    """

    def __init__(self, pending_queue, completed_queue, mq_hostname, rmgr):

        self._uid           = ru.generate_id('radical.entk.task_manager')
        self._logger        = ru.get_logger('radical.entk.task_manager')
        self._prof = ru.Profiler(name = self._uid+'-obj')

        self._prof.prof('create tmgr obj', uid=self._uid)

        self._pending_queue = pending_queue
        self._completed_queue = completed_queue
        self._mq_hostname = mq_hostname
        self._rmgr = rmgr

        self._tmgr_process = None
        self._tmgr_terminate = None
        self._hb_thread = None
        self._hb_alive = None
        self._umgr = None

        self._logger.info('Created task manager object: %s'%self._uid)

        self._prof.prof('tmgr obj created', uid=self._uid)        



    # ------------------------------------------------------------------------------------------------------------------
    # Private Methods
    # ------------------------------------------------------------------------------------------------------------------

    def _heartbeat(self):

        """

        """

        self._prof.prof('heartbeat thread started', uid=self._uid)

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.queue_delete(queue='heartbeat-req')
        channel.queue_declare(queue='heartbeat-req')
        response = True
        
        while (response or (not self._hb_alive.is_set())):
            response = False
            corr_id = str(uuid.uuid4())
        
            # Heartbeat request signal sent to task manager via rpc-queue
            channel.basic_publish(  exchange='',
                                    routing_key='heartbeat-req',
                                    properties=pika.BasicProperties(
                                                reply_to = 'heartbeat-res',
                                                correlation_id = corr_id,
                                                ),
                                    body='request')

            self._logger.info('Sent heartbeat request')

            # Ten second interval for heartbeat request to be responded to
            time.sleep(10)

            method_frame, props, body = channel.basic_get(queue='heartbeat-res')

            if body:
                if corr_id == props.correlation_id:
                    self._logger.info('Received heartbeat response')
                    response = True

                    channel.basic_ack(delivery_tag = method_frame.delivery_tag)



        # Process is dead - close it so that appmanager can restart it
        self.end_manager()
        self._hb_alive.set()

        self._prof.prof('terminating hearbeat thread', uid=self._uid)        

    def start_manager(self):

        # This method starts the extractor function in a separate thread        
        
        if not self._tmgr_process:

            try:

                self._prof.prof('creating tmgr process', uid=self._uid)
                self._tmgr_process = Process(   target=self.tmgr, 
                                                name='task-manager', 
                                                args=(
                                                    self._uid,
                                                    self._umgr, 
                                                    self._rmgr,
                                                    self._logger,
                                                    self._mq_hostname,
                                                    self._pending_queue,
                                                    self._completed_queue)
                                            )

                self._tmgr_terminate = Event()
                self._logger.info('Starting task manager process')
                self._prof.prof('starting tmgr process', uid=self._uid)
                self._tmgr_process.start()                

                return True

            except Exception, ex:

                self.end_manager()
                self._logger.error('Task manager not started')
                raise                

        else:
            self._logger.info('Helper process already running')
            raise


    def end_manager(self):

        # Set termination flag
        try:
            if not self._tmgr_terminate.is_set():
                self._tmgr_terminate.set()

            self._tmgr_process.join()
            self._logger.info('Task manager process closed')

            self._prof.prof('tmgr process terminated', uid=self._uid)

            return True

        except Exception, ex:
            self._logger.error('Could not terminate task manager process')
            raise

  
    def tmgr(self, uid, umgr, rmgr, logger, mq_hostname, pending_queue, completed_queue):

        """
        # This function extracts currently tasks from the pending_queue
        # and pushes it to the executed_queue. Thus mimicking an execution plugin

        """
        
        try:

            local_prof = ru.Profiler(name = self._uid + '-proc')

            local_prof.prof('tmgr process started', uid=self._uid)
            logger.info('Task Manager process started') 


            placeholder_dict = dict()

            def load_placeholder(task):

                parent_pipeline = str(task._parent_pipeline)
                parent_stage = str(task._parent_stage)

                if parent_pipeline not in placeholder_dict:
                    placeholder_dict[parent_pipeline] = dict()

                if parent_stage not in placeholder_dict[parent_pipeline]:
                    placeholder_dict[parent_pipeline][parent_stage] = dict()

                placeholder_dict[parent_pipeline][parent_stage][str(task.uid)] = str(task.path)


            def unit_state_cb(unit, state):

                try:

                    logger.debug('Unit %s in state %s'%(unit.uid, unit.state))

                    # Thread should run till terminate condtion is encountered
                    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_hostname))
                    mq_channel = mq_connection.channel()
                
                    if unit.state in [rp.EXECUTING]:
                        task = create_task_from_cu(unit, local_prof)
                        #print 'Executing: %s %s'%(unit.uid, task.uid)

                    if unit.state in [rp.DONE, rp.FAILED]:

                        task = create_task_from_cu(unit, local_prof)
                        #print 'Done: %s %s'%(unit.uid, task.uid)
                        
                        task.state = states.COMPLETED
                        local_prof.prof('transition', 
                                        uid=task.uid, 
                                        state=task.state,
                                        msg=task._parent_stage)

                        if unit.state == rp.DONE:
                            task.exit_code = 0
                        else:
                            task.exit_code = 1

                        task.path = str(unit.sandbox)

                        load_placeholder(task)

                        sync_with_master(obj=task, obj_type='Task', channel = mq_channel)
                
                        task_as_dict = json.dumps(task.to_dict())

                        mq_channel.basic_publish(   exchange='',
                                                    routing_key='completedq-1',
                                                    body=task_as_dict
                                                        #properties=pika.BasicProperties(
                                                            # make message persistent
                                                            #    delivery_mode = 2, 
                                                        #)
                                                ) 

                        logger.info('Pushed task %s with state %s to completed queue %s'%(
                                                                                    task.uid, 
                                                                                    task.state,
                                                                                    completed_queue[0])
                                                                                    )

                    mq_connection.close()

                except KeyboardInterrupt:
                    self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                            'trying to exit callback thread gracefully...')

                    print traceback.format_exc()

                    raise KeyboardInterrupt

                except Exception, ex:

                    self._logger.error('Callback failed with error: %s'%ex)

                    print traceback.format_exc()

                    raise
            

            if not umgr:
                umgr = rp.UnitManager(session=rmgr._session)
                umgr.add_pilots(rmgr.pilot)
                umgr.register_callback(unit_state_cb)

            # Thread should run till terminate condtion is encountered
            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_hostname))
            mq_channel = mq_connection.channel()

            # To respond to heartbeat - get request from rpc_queue
            mq_channel.queue_delete(queue='heartbeat-res')
            mq_channel.queue_declare(queue='heartbeat-res')

            '''
            # Function to be invoked upon request message
            def on_request(ch, method, props, body):

                self._logger.info('Received heartbeat request')

                ch.basic_publish(   exchange='',
                                    routing_key=props.reply_to,
                                    properties=pika.BasicProperties(correlation_id = props.correlation_id),
                                    body='response')

                self._logger.info('Sent heartbeat response')

                ch.basic_ack(delivery_tag = method.delivery_tag)
            '''

            local_prof.prof('tmgr infrastructure setup done', uid=uid)

            while not self._tmgr_terminate.is_set():

                try:

                    method_frame, header_frame, body = mq_channel.basic_get(queue=pending_queue[0])

                    if body:

                        try:

                            #print 'Body: ',body

                            task = Task()
                            task.from_dict(json.loads(body))

                            task.state = states.SUBMITTING

                            local_prof.prof('transition', 
                                            uid=task.uid, 
                                            state=task.state,
                                            msg=task._parent_stage)

                            sync_with_master(task, 'Task', mq_channel)

                            self._logger.debug('Got task %s from pending_queue %s'%(task.uid, pending_queue[0]))                            

                            self._logger.info('Task %s, %s; submitted to RTS'%(task.uid, task.state))

                            umgr.submit_units(create_cud_from_task(task, placeholder_dict, local_prof))

                            task.state = states.SUBMITTED

                            local_prof.prof('transition', 
                                            uid=task.uid, 
                                            state=task.state,
                                            msg=task._parent_stage)

                            sync_with_master(task, 'Task', mq_channel)

                            mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                        except Exception, ex:

                            # Rolling back queue and task status
                            logger.error('Error while pushing task to completed queue, rolling back: %s'%ex)
                            raise Error(text=ex)
                
                        if slow_run:
                            time.sleep(1)

                    # Get request from rpc_queue for heartbeat response
                    method_frame, props, body = mq_channel.basic_get(queue='heartbeat-req')

                    if body:

                        logger.info('Received heartbeat request')

                        mq_channel.basic_publish(   exchange='',
                                    routing_key='heartbeat-res',
                                    properties=pika.BasicProperties(correlation_id = props.correlation_id),
                                    body='response')

                        logger.info('Sent heartbeat response')
                        mq_channel.basic_ack(delivery_tag = method_frame.delivery_tag)
            

                except Exception, ex:

                    logger.error('Error getting messages from pending queue: %s'%ex)
                    print traceback.format_exc()
                    raise Error(text=ex) 

            local_prof.prof('terminating tmgr process', uid=uid)

            local_prof.close()

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                'trying to cancel enqueuer thread gracefully...')
            raise KeyboardInterrupt


        except Exception, ex:

            self._logger.error('Unknown error in helper process: %s'%ex)
            print traceback.format_exc()
            raise Error(text=ex)


    # ------------------------------------------------------------------------------------------------------------------
    # Public Methods
    # ------------------------------------------------------------------------------------------------------------------

    def start_heartbeat(self):

        """
        **Purpose**: Method to start the heartbeat thread. The heartbeat function
        is not to be accessed directly. The function is started in a separate
        thread using this method.
        """

        self._logger.info('Starting hearbeat thread')
        self._prof.prof('creating heartbeat thread', uid=self._uid)
        self._hb_thread = threading.Thread(target=self._heartbeat, name='heartbeat')
        self._hb_alive = threading.Event()
        self._prof.prof('starting heartbeat thread', uid=self._uid)
        self._hb_thread.start()


    def end_heartbeat(self):

        """
        **Purpose**: Method to terminate the heartbeat thread. This method is 
        blocking as it waits for the heartbeat thread to terminate (aka join).

        This is the last method that is executed from the TaskManager and
        hence closes the profiler.
        """

        try:

            if self._hb_thread.is_alive():
                self._hb_alive.set()
                self._hb_thread.join()

            self._logger.info('Hearbeat thread terminated')

            self._prof.prof('hearbeat thread terminated', uid=self._uid)

            # We close in the hearbeat because it is ends after the mgr process
            self._prof.close()

            return True

        except Exception, ex:
            self._logger.error('Could not terminate hearbeat thread')
            raise
          

    def start_manager(self):

        """
        **Purpose**: Method to start the tmgr process. The tmgr process
        is not to be accessed directly. The function is started in a separate
        process using this method.
        """
        
        if not self._tmgr_process:

            try:

                self._prof.prof('creating tmgr process', uid=self._uid)
                self._tmgr_process = Process(target=self._tmgr, name='task-manager')
                self._tmgr_terminate = Event()
                self._logger.info('Starting task manager process')
                self._prof.prof('starting tmgr process', uid=self._uid)
                self._tmgr_process.start()                

            except Exception, ex:

                self.end_manager()
                self._logger.error('Task manager not started')
                raise                

        else:
            self._logger.info('Helper process already running')
            raise


    def end_manager(self):

        """
        **Purpose**: Method to terminate the tmgr process. This method is 
        blocking as it waits for the tmgr process to terminate (aka join).
        """

        try:
            if not self._tmgr_terminate.is_set():
                self._tmgr_terminate.set()

            self._tmgr_process.join()
            self._logger.info('Task manager process closed')

            self._prof.prof('tmgr process terminated', uid=self._uid)

        except Exception, ex:
            self._logger.error('Could not terminate task manager process')
            raise     


    def check_alive(self):

        """
        **Purpose**: Check if the tmgr process is alive and running
        """

        return self._tmgr_process.is_alive()


    def check_heartbeat(self):

        """
        **Purpose**: Check if the heartbeat thread is alive and running
        """

        return self._hb_thread.is_alive()

    # ------------------------------------------------------------------------------------------------------------------