
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


""" 
And example on using the radical.utils.config tools. 

This example will read config options fomr $HOME/.examples.cfg::
    
    [config]
    casing   = upper
    excluded = sparks,pftools
    
    [sp3.cd]
    exe  = /usr/local/bin/sp3
    args = no,idea

That setting can be overwritting via $EXAMPLE_CONFIG_CASING.

"""

import radical.utils.config  as ruc


# ------------------------------------------------------------------------------
#
# a set of pre-defined options
#
_config_options = [
    { 
    'category'      : 'config',
    'name'          : 'casing', 
    'type'          : str, 
    'default'       : 'default',
    'valid_options' : ['default', 'lower', 'upper'],
    'documentation' : "This option determines the casing of example's output",
    'env_variable'  : 'EXAMPLE_CONFIG_CASING'
    },
    { 
    'category'      : 'config',
    'name'          : 'excluded', 
    'type'          : list, 
    'default'       : '',
    'valid_options' : [],
    'documentation' : "This option determines set of excluded components",
    'env_variable'  : ''
    }
]

_sp3_options = [
    { 
    'category'      : 'sp3.cd',
    'name'          : 'exe', 
    'type'          : str, 
    'default'       : '/usr/bin/sp3',
    'valid_options' : [],
    'documentation' : "This option determines set sp3 executable ",
    'env_variable'  : ''
    },
    { 
    'category'      : 'sp3.cd',
    'name'          : 'args', 
    'type'          : list, 
    'default'       : '',
    'valid_options' : [],
    'documentation' : "This option determines set sp3 arguments ",
    'env_variable'  : ''
    }
]


# ------------------------------------------------------------------------------
#
class FancyEcho (ruc.Configurable): 
    """ 
    This example will evaluate the given configuration, and 
    """

    #-----------------------------------------------------------------
    # 
    def __init__(self):
        
        # set the configuration options for this object
        ruc.Configurable.__init__ (self, 'examples')
        ruc.Configurable.config_options (self, 'config', _config_options)
        ruc.Configurable.config_options (self, 'sp3.cd', _sp3_options)

        # use the configuration
        self._cfg = self.get_config ('config')

        self._mode = self._cfg['casing'].get_value ()
        print "mode: %s" % self._mode

        self._excl = self._cfg['excluded'].get_value ()
        print "excl: %s" % type(self._excl)
        print "excl: %s" % self._excl


        if  not 'sp3' in self._excl :
            print 'running sp3'

        if  not 'sparks' in self._excl :
            print 'running sparks'

        if  not 'pftools' in self._excl :
            print 'running pftools'

        # use sp3 configuration
        self._sp3 = self.get_config ('sp3.cd')
        print self._sp3['exe'].get_value ()
        print self._sp3['args'].get_value ()


    #-----------------------------------------------------------------
    # 
    def echo (self, source) :

        target = ""

        if  self._mode == 'default' :
            target = source

        elif self._mode == 'lower' :
            target = source.lower()

        elif self._mode == 'upper' :
            target = source.upper()

        else :
            target = 'cannot handle mode %s' % self._mode

        return target


# ------------------------------------------------------------------------------
#

fc  = FancyEcho ()
src = ''

while 'quit' != src :
    src = raw_input ('> ')
    tgt = fc.echo (src)
    print '  ' + tgt


# ------------------------------------------------------------------------------
#



