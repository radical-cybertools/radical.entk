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
from task_processor import create_compute_unit, create_task

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

        self._umgr = rp.UnitManager(session=self._rmgr._session)
        self._umgr.add_pilots(self._rmgr.pilot)


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


    def create_cu_from_task(self, task):

        return create_compute_unit(task)

    def create_task_from_cu(self, cu):

        return create_task(cu)


    def unit_state_cb(self, unit, state):


        try:

            # Thread should run till terminate condtion is encountered
            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname))
            mq_channel = mq_connection.channel()

            self._logger.debug('Unit %s in state %s'%(unit.uid, unit.state))

            if unit.state == rp.DONE:

                task = self.create_task_from_cu(unit)
                task.state = states.DONE
                    
                task_as_dict = json.dumps(task.to_dict())

                mq_channel.basic_publish(   exchange='fork',
                                                    routing_key='',
                                                    body=task_as_dict
                                                    #properties=pika.BasicProperties(
                                                        # make message persistent
                                                        #    delivery_mode = 2, 
                                                    #)
                                                ) 

                self._logger.debug('Pushed task %s with state %s to completed queue %s and synchronizerq'%(
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

    def tmgr(self):

        # This function extracts currently tasks from the pending_queue
        # and pushes it to the executed_queue. Thus mimicking an execution plugin

        try:

            self._logger.info('Task Manager process started') 

            # Thread should run till terminate condtion is encountered
            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname))
            mq_channel = mq_connection.channel()

            self._umgr.register_callback(self.unit_state_cb)


            while not self._tmgr_terminate.is_set():

                try:

                    method_frame, header_frame, body = mq_channel.basic_get(queue=self._pending_queue[0])

                    if body:


                        try:

                            task = Task()
                            task.load_from_dict(json.loads(body))

                            self._logger.debug('Got task %s from pending_queue %s'%(task.uid, self._pending_queue[0]))
                            
                            task.state = states.SCHEDULED

                            self._logger.debug('Task %s, %s; submitted to RTS'%(task.uid, task.state))

                            self._umgr.submit_units(self.create_cu_from_task(task))

                            mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                        except Exception, ex:

                            # Rolling back queue and task status
                            self._logger.error('Error while pushing task to completed queue, rolling back: %s'%ex)
                            raise UnknownError(text=ex)
                
                        if slow_run:
                            time.sleep(1)

                


                except Exception, ex:

                    self._logger.error('Error getting messages from pending queue: %s'%ex)
                    raise UnknownError(text=ex) 


        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                'trying to cancel enqueuer thread gracefully...')
            raise KeyboardInterrupt


        except Exception, ex:

            self._logger.error('Unknown error in helper process: %s'%ex)
            print traceback.format_exc()
            raise UnknownError(text=ex) 


    def check_alive(self):

        return self._tmgr_process.is_alive()