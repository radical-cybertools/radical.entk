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

slow_run = os.environ.get('RADICAL_ENTK_SLOW',False)

# TEMPORARY FILE TILL AN EXECUTION PLUGIN IS IN PLACE

class Helper(object):

    def __init__(self, pending_queue, completed_queue, mq_hostname, resource_desc):

        self._uid           = ru.generate_id('radical.entk.helper')
        self._logger        = ru.get_logger('radical.entk.helper')

        self._pending_queue = pending_queue
        self._completed_queue = completed_queue
        self._mq_hostname = mq_hostname

        self._helper_process = None

        self._resource_desc = resource_desc

        self._logger.info('Created helper object: %s'%self._uid)


    def start_helper(self):

        # This method starts the extractor function in a separate thread        
        
        if not self._helper_process:

            try:

                # Attempt to start RP pilot on 


            try:
                self._helper_process = Process(target=self.helper, name='helper')
                self._helper_terminate = Event()
                self._logger.info('Starting helper process')
                self._helper_process.start()                

                return True

            except Exception, ex:

                self.end_helper()
                self._logger.error('Helper process not started')
                raise

                

        else:
            self._logger.info('Helper process already running')
            raise


    def end_helper(self):

        # Set termination flag
        try:
            if not self._helper_terminate.is_set():
                self._helper_terminate.set()

            self._helper_process.join()
            self._logger.info('Helper thread closed')

            return True

        except Exception, ex:
            self._logger.error('Could not terminate helper process')
            raise



    def helper(self):

        # This function extracts currently tasks from the pending_queue
        # and pushes it to the executed_queue. Thus mimicking an execution plugin

        try:

            self._logger.info('Helper process started')

            # Thread should run till terminate condtion is encountered
            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname))
            mq_channel = mq_connection.channel()

            while not self._helper_terminate.is_set():

                try:

                    method_frame, header_frame, body = mq_channel.basic_get(queue=self._pending_queue[0])

                    if body:


                        try:
                            task = Task()
                            task.load_from_dict(json.loads(body))

                            task.state = states.DONE

                            task_as_dict = json.dumps(task.to_dict())

                            self._logger.debug('Got task %s from pending_queue %s'%(task.uid, self._pending_queue[0]))

                            mq_channel.basic_publish( exchange='fork',
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

        return self._helper_process.is_alive()