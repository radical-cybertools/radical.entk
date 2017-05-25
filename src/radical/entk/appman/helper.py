__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
from multiprocessing import Process
import Queue
from radical.entk import states, Task
import time
import json

# TEMPORARY FILE TILL AN EXECUTION PLUGIN IS IN PLACE

class Helper(object):

    def __init__(self, pending_queue, executed_queue, channel):

        self._uid           = ru.generate_id('radical.entk.helper')
        self._logger        = ru.get_logger('radical.entk.helper')

        self._pending_queue = pending_queue
        self._executed_queue = executed_queue
        self._channel = channel

        self._helper_process = None

        self._logger.info('Created helper object: %s'%self._uid)


    def start_helper(self):

        # This method starts the extractor function in a separate thread

        self._logger.info('Starting helper process')
        
        if not self._helper_process:


            try:
                self._helper_process = Process(target=self.helper, name='helper')
                self._helper_terminate = threading.Event()
                self._helper_process.start()

                self._logger.info('WFprocessor started')

                return True

            except Exception, ex:

                self.end_helper()
                self._logger.error('WFprocessor not started')
                raise

                

        else:
            self._logger.info('WFprocessor already running')
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

            # Thread should run till terminate condtion is encountered
            while not self._helper_terminate.is_set():

                try:

                    method_frame, header_frame, body = channel.basic_get(queue=self._pending_queue[0])

                    if body:


                        try:
                            task = Task()
                            task.load_from_dict(json.loads(task))

                            task.state = states.DONE

                            task_as_dist = json.dumps(task.to_dict())

                            self._logger.debug('Got task %s from pending queue'%(task.uid))                    

                            self._mq_channel.basic_publish( exchange='fork',
                                                            routing_key='',
                                                            body=task_as_dict,
                                                            properties=pika.BasicProperties(
                                                                # make message persistent
                                                                delivery_mode = 2, 
                                                            )
                                                        )                    
                        
                            self._logger.debug('Pushed task %s with state %s to executed queue'%(task.uid, task.state))

                            self._mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                        except Exception, ex:

                            # Rolling back queue and task status
                            self._logger.error('Error while pushing task to completed queue, rolling back')
                
                    time.sleep(1)

                


                except Exception, ex:

                    self._logger.error('Error getting messages from pending queue'%ex)
                    raise UnknownError(text=ex) 


        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                'trying to cancel enqueuer thread gracefully...')


        except Exception, ex:

            self._logger.error('Unknown error in helper process: %s'%ex)
            raise UnknownError(text=ex) 


   def check_alive(self):

        return self._thread_alive