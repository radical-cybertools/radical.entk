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

# TEMPORARY FILE TILL AN EXECUTION PLUGIN IS IN PLACE

class Helper(object):

    def __init__(self, pending_queue, completed_queue, channel):

        self._uid           = ru.generate_id('radical.entk.helper')
        self._logger        = ru.get_logger('radical.entk.helper')

        self._pending_queue = pending_queue
        self._completed_queue = completed_queue
        self._channel = channel

        self._helper_process = None

        self._logger.info('Created helper object: %s'%self._uid)


    def start_helper(self):

        # This method starts the extractor function in a separate thread        
        
        if not self._helper_process:


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
            mq_connection = pika.BlockingConnection(
                                    pika.ConnectionParameters(host='localhost')
                                    )
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

                            self._logger.debug('Got task %s from pending queue'%(task.uid))                    

                            mq_channel.basic_publish( exchange='fork',
                                                            routing_key='',
                                                            body=task_as_dict
                                                            #properties=pika.BasicProperties(
                                                                # make message persistent
                                                            #    delivery_mode = 2, 
                                                            #)
                                                        )                    
                        
                            self._logger.debug('Pushed task %s with state %s to completed queue'%(task.uid, task.state))

                            mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                        except Exception, ex:

                            # Rolling back queue and task status
                            self._logger.error('Error while pushing task to completed queue, rolling back: %s'%ex)
                            raise UnknownError(text=ex)
                
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


