import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk.task.task import Task
from radical.entk import states


class Stage(object):

    def __init__(self):

        self._uid       = ru.generate_id('radical.entk.stage')
        self._tasks     = set()
        self._name      = str()

        self._state     = states.NEW

        # To change states
        self._task_count = len(self._tasks)

        # Pipeline this stage belongs to
        self._parent_pipeline = None


    def validate_tasks(self, tasks):

        if not isinstance(tasks, set):

            if not isinstance(tasks, list):
                tasks = set([tasks])
            else:
                tasks = set(tasks)

        for val in tasks:

            if not isinstance(val, Task):
                raise TypeError(expected_type=Task, actual_type=type(val))

        return tasks

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

    @property
    def parent_pipeline(self):
        return self._parent_pipeline

    @property
    def uid(self):
        return self._uid   
    # -----------------------------------------------


    # -----------------------------------------------
    # Setter functions
    # -----------------------------------------------

    @name.setter
    def name(self, value):
        if isinstance(value,str):
            self._name = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))
        

    @tasks.setter
    def tasks(self, tasks):        
        self._tasks = self.validate_tasks(tasks)

    @parent_pipeline.setter
    def parent_pipeline(self, value):
        if isinstance(value, str):
            self._parent_pipeline = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))

    @state.setter
    def state(self, value):
        if isinstance(value,str):
            self._state = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))        
    # -----------------------------------------------


    def add_tasks(self, tasks):
        tasks = self.validate_tasks(tasks)
        self._tasks.update(tasks)
        


    def remove_tasks(self, task_names):

        if not isinstance(task_names, list):
            task_names = [task_names]

        for val in task_names:
            if not isinstance(val, str):
                raise TypeError(expected_type=str, actual_type=type(val))


        copy_of_existing_tasks = self._tasks
        copy_task_names = task_names

        for task in self._tasks:

            for task_name in task_names:
                
                if task.name ==  task_name:

                    copy_of_existing_tasks.remove(task)
                    copy_task_names.remove(task)

            task_names = copy_task_names

        self._tasks = copy_of_existing_tasks


    def pass_uid(self, tasks=None):

        if tasks is None:
            for task in self._tasks:
                task.parent_stage = self._uid
                task.parent_pipeline = self._parent_pipeline
        else:
            for task in tasks:
                task.parent_stage = self._uid
                task.parent_pipeline = self._parent_pipeline


            return tasks

    def set_task_state(self, value):

        if isinstance(value, str):
            for task in self._tasks:
                task.state = state

        else:
            raise TypeError(expected_type=str, actual_type=type(value))

    def check_tasks_status(self):

        try:

            for task in self._tasks:
                if task.state not in [states.DONE, states.FAILED]:
                    return False
                    
            return True

        except Exception, ex:

            print 'Task state evaluation failed'
            raise UnknownError(text=ex)