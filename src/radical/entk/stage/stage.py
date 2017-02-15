import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk.task.task import Task

class Stage(object):

    def __init__(self, tasks, name):

        self._uid       = ru.generate_id('radical.entk.stage')
        self._tasks    = tasks
        self._name      = name

        self._state     = 'New'

        self.validate_args()

        # To change states
        self._task_count = len(self._tasks)


    def validate_args(self):

        if not isinstance(self._tasks, list):
            self._tasks = [self._tasks]

        for val in self._tasks:

            if not isinstance(val, Task):
                raise TypeError(expected_type=Task, actual_type=type(val))

    # -----------------------------------------------
    # Getter functions
    # -----------------------------------------------

    @property
    def name(self):
        return self._name
    
    @property
    def tasks(self):
        return self._tasks

    @property
    def state(self):
        return self._state

    # -----------------------------------------------


    # -----------------------------------------------
    # Setter functions
    # -----------------------------------------------

    @tasks.setter
    def tasks(self, values):

        self._tasks = values
        self.validate_args()
    # -----------------------------------------------


    def add_tasks(self, tasks):

        if not isinstance(tasks, list):
            tasks = [tasks]

        self._tasks.extend(tasks)


    def remove_tasks(self, task_names):

        if not isinstance(task_names, list):
            task_names = [task_names]

        copy_of_existing_tasks = self._tasks
        copy_task_names = task_names

        for task in self._tasks:

            for task_name in task_names:
                
                if task.name ==  task_name:

                    copy_of_existing_tasks.remove(task)
                    copy_task_names.remove(task)

            task_names = copy_task_names

        self._tasks = copy_of_existing_tasks
