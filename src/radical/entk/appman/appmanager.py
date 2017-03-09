__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk.pipeline.pipeline import Pipeline
from populator import Populator
import sys

class AppManager(object):

    def __init__(self, name=None):

        self._uid       = ru.generate_id('radical.entk.appmanager')
        self._name      = name

        self._workload = None

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

    def assign_workload(self, workload):

        try:

            if isinstance(workload, Pipeline):
                self._workload = set(workload)

            if isinstance(workload, set):
                for item in workload:
                    if not isinstance(item, Pipeline):
                        self._logger.info('Workload type incorrect')
                        raise TypeError(expected_type=['pipeline', 'set of pipelines'], actual_type=type(workload))

                self._workload = workload

        except Exception, ex:

            self._logger.error('Fatal error while adding workload to appmanager: %s'%ex)
            raise

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), tring to exit gracefully...')
            sys.exit(1)


    def run(self):

        try:

            if self._workload is None:
                print 'Assign workload before invoking run method - cannot proceed'
                self._logger.info('No workload assigned currently, please check your script')
                pass

            else:

                populator = Populator(workload = self._workload)
                intermediate_q1 = populator.start_population()

                updater = Updater(workload = self._workload, executed_queue = intermediate_q1)
                updater.start_update()



        except Exception, ex:

            self._logger.error('Fatal error while adding workload to appmanager: %s'%ex)
            raise

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                            'trying to exit gracefully...')
            sys.exit(1)
