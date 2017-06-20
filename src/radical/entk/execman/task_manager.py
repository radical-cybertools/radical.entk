__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
from multiprocessing import Process, Event
import Queue
from radical.entk import states, Task
import time
import json
import pika
import traceback
import os
import radical.pilot as rp
from task_processor import create_cud_from_task, create_task_from_cu

slow_run = os.environ.get('RADICAL_ENTK_SLOW',False)

class TaskManager(object):

    def __init__(self, pending_queue, completed_queue, mq_hostname, rmgr):

        self._uid           = ru.generate_id('radical.entk.task_manager')
        self._logger        = ru.get_logger('radical.entk.task_manager')

        self._pending_queue = pending_queue
        self._completed_queue = completed_queue
        self._mq_hostname = mq_hostname
        self._rmgr = rmgr

        self._tmgr_process = None

        self._logger.info('Created task manager object: %s'%self._uid)

        self._setup()


    def _setup(self):

        pass


    def start_monitor(self):

        self._logger.info('Starting hearbeat thread')
        self._hb_thread = threading.Thread(target=self.monitor, name='heartbeat')
        self._hb_alive = threading.Event()
        self._hb_thread.start()


    def end_monitor(self):

        try:

            if self._hb_thread.is_alive():
                self._hb_alive.set()
                self._hb_thread.join()

            self._logger.info('Hearbeat thread terminated')

            return True

        except Exception, ex:
            self._logger.error('Could not terminate hearbeat thread')
            raise

    def monitor(self):

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        result = channel.queue_declare(exclusive=True)
        callback_queue = result.method.queue
        response = True
        

        #def on_response(ch, method, props, body):
        #    if corr_id == props.correlation_id:
        #        response = True

        #    self._logger.info('Received heartbeat response')

        while (response or (not self._hb_alive.is_set())):
            response = False
            corr_id = str(uuid.uuid4())
        
            # Heartbeat request signal sent to task manager via rpc-queue
            channel.basic_publish(  exchange='',
                                    routing_key='rpc_queue',
                                    properties=pika.BasicProperties(
                                                reply_to = callback_queue,
                                                correlation_id = corr_id,
                                                ),
                                    body='request')

            self._logger.info('Sent heartbeat request')

            # Ten second interval for heartbeat request to be responded to
            time.sleep(10)

            method_frame, props, body = channel.basic_get(no_ack=True, queue=callback_queue)

            if body:
                if corr_id == props.correlation_id:
                self._logger.info('Received heartbeat response')
                response = True



        # Process is dead - close it so that appmanager can restart it
        self.end_manager()
        self._hb_alive.set()

    def start_manager(self):

        # This method starts the extractor function in a separate thread        
        
        if not self._tmgr_process:

            try:
                self._tmgr_process = Process(target=self.tmgr, name='task-manager')
                self._tmgr_terminate = Event()
                self._logger.info('Starting task manager process')
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

            return True

        except Exception, ex:
            self._logger.error('Could not terminate task manager process')
            raise

  
    def tmgr(self):

        # This function extracts currently tasks from the pending_queue
        # and pushes it to the executed_queue. Thus mimicking an execution plugin

        try:

            def unit_state_cb(unit, state):

                try:


                    # Thread should run till terminate condtion is encountered
                    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname))
                    mq_channel = mq_connection.channel()

                    self._logger.debug('Unit %s in state %s'%(unit.uid, unit.state))

                    if unit.state in [rp.DONE, rp.FAILED]:

                        task = create_task_from_cu(unit)
                        task.state = states.COMPLETED

                        if unit.state == rp.DONE:
                            task.exit_code = 0
                        else:
                            task.exit_code = 1
                    
                        task_as_dict = json.dumps(task.to_dict())

                        mq_channel.basic_publish(   exchange='',
                                                    routing_key='completedq-1',
                                                    body=task_as_dict
                                                        #properties=pika.BasicProperties(
                                                            # make message persistent
                                                            #    delivery_mode = 2, 
                                                        #)
                                                ) 

                        self._logger.debug('Pushed task %s with state %s to completed queue %s'%(
                                                                                    task.uid, 
                                                                                    task.state,
                                                                                    self._completed_queue[0])
                                                                                    )

                    mq_connection.close()

                except KeyboardInterrupt:
                    self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                            'trying to exit callback thread gracefully...')
                    raise KeyboardInterrupt

                except Exception, ex:

                    self._logger.error('Callback failed with error: %s'%ex)
                    raise

            self._logger.info('Task Manager process started') 

            self._umgr = rp.UnitManager(session=self._rmgr._session)
            self._umgr.add_pilots(self._rmgr.pilot)
            self._umgr.register_callback(unit_state_cb)

            # Thread should run till terminate condtion is encountered
            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname))
            mq_channel = mq_connection.channel()

            # To respond to heartbeat - get request from rpc_queue
            mq_channel.queue_declare(queue='rpc_queue')

            # Function to be invoked upon request message
            def on_request(ch, method, props, body):

                self._logger.info('Received heartbeat request')

                ch.basic_publish(   exchange='',
                                    routing_key=props.reply_to,
                                    properties=pika.BasicProperties(correlation_id = props.correlation_id),
                                    body='response')

                self._logger.info('Sent heartbeat response')

                ch.basic_ack(delivery_tag = method.delivery_tag)

            while not self._tmgr_terminate.is_set():

                try:

                    method_frame, header_frame, body = mq_channel.basic_get(queue=self._pending_queue[0])

                    if body:


                        try:

                            task = Task()
                            task.from_dict(json.loads(body))

                            task.state = states.SUBMITTING

                            self._logger.debug('Got task %s from pending_queue %s'%(task.uid, self._pending_queue[0]))                            

                            self._logger.debug('Task %s, %s; submitted to RTS'%(task.uid, task.state))

                            self._umgr.submit_units(create_cud_from_task(task))

                            task.state = states.SUBMITTED

                            mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                        except Exception, ex:

                            # Rolling back queue and task status
                            self._logger.error('Error while pushing task to completed queue, rolling back: %s'%ex)
                            raise Error(text=ex)
                
                        if slow_run:
                            time.sleep(1)

                    # Get request from rpc_queue for heartbeat response
                    method_frame, props, body = mq_channel.basic_get(queue='rpc_queue')

                    if body:

                        self._logger.info('Received heartbeat request')

                        mq_channel.basic_publish(   exchange='',
                                    routing_key=props.reply_to,
                                    properties=pika.BasicProperties(correlation_id = props.correlation_id),
                                    body='response')

                        self._logger.info('Sent heartbeat response')
                        mq_channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                


                except Exception, ex:

                    self._logger.error('Error getting messages from pending queue: %s'%ex)
                    raise Error(text=ex) 


        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                'trying to cancel enqueuer thread gracefully...')
            raise KeyboardInterrupt


        except Exception, ex:

            self._logger.error('Unknown error in helper process: %s'%ex)
            print traceback.format_exc()
            raise Error(text=ex) 


    def check_alive(self):

        return self._tmgr_process.is_alive()