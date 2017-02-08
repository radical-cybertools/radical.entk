import radical.utils as ru

class Task(object):

    def __init__(self, name):

        self._uid       = ru.generate_id('radical.entk.task')
        self._name      = name

        self._state     = 'New'

        # Attributes necessary for execution
        self._environment   = None
        self._executable    = None
        self._arguments     = None
        
        # Data staging attributes
        self._upload_input_data     = None
        self._copy_input_data       = None
        self._link_input_data       = None
        self._copy_output_data      = None
        self._download_output_data  = None


    # -----------------------------------------------
    # Getter functions
    # -----------------------------------------------

    @property
    def name(self):
        return self._name
    
    @property
    def state(self):
        return self._state

    @property
    def environment(self):
        return self._environment
    
    @property
    def executable(self):
        return self._executable
    
    @property
    def arguments(self):
        return self._arguments
    
    @property
    def upload_input_data(self):
        return self._upload_input_data
    
    @property
    def copy_input_data(self):
        return self._copy_input_data
    
    @property
    def link_input_data(self):
        return self._link_input_data
    
    @property
    def copy_output_data(self):
        return self._copy_output_data
    
    @property
    def download_output_data(self):
        return self._download_output_data
    # -----------------------------------------------


    # -----------------------------------------------
    # Setter functions
    # -----------------------------------------------

    @name.setter
    def name(self, value):
        self._name = value

    @environment.setter
    def environment(self, value):
        self._environment = value

    @executable.setter
    def executable(self, value):
        self._executable = value

    @arguments.setter
    def arguments(self, value):
        self._arguments = value

    @upload_input_data.setter
    def upload_input_data(self, value):
        self._upload_input_data = value

    @copy_input_data.setter
    def copy_input_data(self, value):
        self._copy_input_data = value

    @link_input_data.setter
    def link_input_data(self, value):
        self._link_input_data = value

    @copy_output_data.setter
    def copy_output_data(self, value):
        self._copy_output_data = value

    @download_output_data.setter
    def download_output_data(self, value):
        self._download_output_data = value
    # -----------------------------------------------

    