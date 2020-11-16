
__copyright__ = 'Copyright 2017-2018, http://radical.rutgers.edu'
__author__    = 'Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>'
__license__   = 'MIT'


from ..base.resource_manager import Base_ResourceManager


# ------------------------------------------------------------------------------
#
class ResourceManager(Base_ResourceManager):
    '''
    A resource manager takes the responsibility of placing resource requests on
    different, possibly multiple, DCIs. This ResourceManager uses mocks an
    implementation by doing nothing, it is only usable for testing.

    :arguments:
        :resource_desc: dictionary with details of the resource request and
                        access credentials of the user
        :example: resource_desc = {
                                    |  'resource'      : 'xsede.stampede',
                                    |  'walltime'      : 120,
                                    |  'cpus'          : 64,
                                    |  'project'       : 'TG-abcxyz',
                                    |  'queue'         : 'abc',    # optional
                                    |  'access_schema' : 'ssh'     # optional}
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, resource_desc, sid, rts_config):

        super(ResourceManager, self).__init__(resource_desc=resource_desc,
                                              sid=sid,
                                              rts='mock',
                                              rts_config=rts_config)


    # --------------------------------------------------------------------------
    #
    def get_resource_allocation_state(self):
        '''
        **Purpose**: get the state of the resource allocation

        '''
        return None


    # --------------------------------------------------------------------------
    #
    def get_completed_states(self):
        '''
        **Purpose**: test if a resource allocation was submitted

        '''
        return list()


    # --------------------------------------------------------------------------
    #
    def _validate_resource_desc(self):
        '''
        **Purpose**: validate the provided resource description
        '''

        return True


    # --------------------------------------------------------------------------
    #
    def _populate(self):
        '''
        **Purpose**: evaluate attributes provided in the resource description
        '''

        return None


    # --------------------------------------------------------------------------
    #
    def _submit_resource_request(self):
        '''
        **Purpose**: Create a resourceas per provided resource description
        '''

        return None


    # --------------------------------------------------------------------------
    #
    def _terminate_resource_request(self):
        '''
        **Purpose**: Cancel the resource
        '''

        return None


# ------------------------------------------------------------------------------

