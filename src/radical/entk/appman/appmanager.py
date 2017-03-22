__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk.pipeline.pipeline import Pipeline
from populator import Populator
from updater import Updater
from helper import Helper
import sys, time
import Queue


class AppManager(object):

    def __init__(self):

        self._uid       = ru.generate_id('radical.entk.appmanager')
        self._name      = str()

        self._workload  = None


        # Queues
        self._pending_queue = Queue.Queue()
        self._executed_queue = Queue.Queue()

        # Logger
        self._logger = ru.get_logger('radical.entk.appmanager')
        self._logger.info('Application Manager initialized')

    # -----------------------------------------------
    # Getter functions
    # -----------------------------------------------

    @property
    def name(self):
        return self._name


    # -----------------------------------------------
    # Setter functions
    # -----------------------------------------------

    @name.setter
    def name(self, value):
        self._name = value


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
                raise TypeError(expected_type=['Pipeline', 'set of Pipeline'], actual_type=type(item))

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

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), tring to exit gracefully...')
            sys.exit(1)


    def run(self):

        try:

            if self._workload is None:
                print 'Assign workload before invoking run method - cannot proceed'
                self._logger.info('No workload assigned currently, please check your script')
                raise ValueError(expected_value='set of pipelines', actual_value=None)

            else:

                populator = Populator(workload = self._workload, pending_queue=self._pending_queue)
                populator.start_population()

                helper = Helper(pending_queue = self._pending_queue, executed_queue=self._executed_queue)
                helper.start_helper()

                updater = Updater(workload = self._workload, executed_queue = self._executed_queue)
                updater.start_update()

                pipe_count = len(self._workload)
                while pipe_count > 0:
                    time.sleep(1)

                    for pipe in self._workload:
                        if pipe.completed:
                            pipe_count -= 1


                # Terminate threads
                self._logger.info('Closing populator thread')
                populator.terminate()
                self._logger.info('Populator thread closed')
                self._logger.info('Closing updater thread')
                updater.terminate()
                self._logger.info('Updater thread closed')
                self._logger.info('Closing helper thread')
                helper.terminate()
                self._logger.info('Helper thread closed')


        except Exception, ex:

            self._logger.error('Fatal error while running appmanager')
            # Terminate threads
            self._logger.info('Closing populator thread')
            populator.terminate()
            self._logger.info('Populator thread closed')
            self._logger.info('Closing updater thread')
            updater.terminate()
            self._logger.info('Updater thread closed')
            self._logger.info('Closing helper thread')
            helper.terminate()
            self._logger.info('Helper thread closed')
            raise UnknownError(text=ex)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                            'trying to exit gracefully...')
            # Terminate threads
            self._logger.info('Closing populator thread')
            populator.terminate()
            self._logger.info('Populator thread closed')
            self._logger.info('Closing updater thread')
            updater.terminate()
            self._logger.info('Updater thread closed')
            self._logger.info('Closing helper thread')
            helper.terminate()
            self._logger.info('Helper thread closed')

            sys.exit(1)
