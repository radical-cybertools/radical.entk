__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"
__author__    = "Ole Weidner <ole.weidner@rutgers.edu>"

from logger            import logger
from loader            import kerneldict
import saga.attributes as attributes

# ------------------------------------------------------------------------------
# MDTaskDescription attribute description keys
KERNEL                 = 'kernel'
MIN_VERSION            = 'min_version'
ARGUMENTS              = 'arguments'
INPUT_DATA             = 'input_data'
OUTPUT_DATA            = 'output_data'
COPY_LOCAL_INPUT_DATA  = 'copy_local_input_data'
PURGE                  = 'purge'

# ------------------------------------------------------------------------------
# Additional BoundMDTask attribute description keys
ENVIRONMENT            = 'environment'
PRE_EXEC               = 'pre_exec'
POST_EXEC              = 'post_exec'
EXECUTABLE             = 'executable'
MPI                    = 'mpi'
RESOURCE               = 'resource'

# ------------------------------------------------------------------------------
#
class BoundMDTask(attributes.Attributes) :
    """A BoundMDTask object is a read-only data-strucutre that defines specific 
       executables and arguments for an MD kernel / engine invocation on a 
       specific resource.

    .. data:: executable 

       (`Attribute`) The executable (`string`).

    .. data:: arguments 

       (`Attribute`) The arguments to the executable (`list of strings`).

    .. data:: resource 

       (`Attribute`) The resource this BoundMDTask was bound to (`string`).

    .. data:: input_data 

       (`Attribute`) The input files that need to be transferred before execution (`transfer directive string`).

    .. data:: output_data 

       (`Attribute`) The output files that need to be transferred back after execution (`transfer directive string`).

    """
    def __init__(self, _environment, _pre_exec, _post_exec, _executable, _mpi, _arguments, _resource, _input_data, _output_data):
        """Le constructeur.
        """ 

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        self._attributes_register(ENVIRONMENT,             _environment,            attributes.STRING, attributes.VECTOR, attributes.READONLY)
        self._attributes_register(PRE_EXEC,                _pre_exec,               attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_register(POST_EXEC,               _post_exec,              attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_register(EXECUTABLE,              _executable,             attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_register(MPI,                     _mpi,                    attributes.BOOL,   attributes.SCALAR, attributes.READONLY)
        self._attributes_register(ARGUMENTS,               _arguments,              attributes.STRING, attributes.VECTOR, attributes.READONLY)
        self._attributes_register(RESOURCE,                _resource,               attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_register(INPUT_DATA,              _input_data,             attributes.STRING, attributes.VECTOR, attributes.READONLY)
        self._attributes_register(OUTPUT_DATA,             _output_data,            attributes.STRING, attributes.VECTOR, attributes.READONLY)

# ------------------------------------------------------------------------------
#
class MDTaskDescription(attributes.Attributes) :
    """An MDTaskDescription object describes a single MD step (kernel / engine
       invocation) independently from any system specifics, like paths, etc.

    .. note:: An MDTaskDescription **MUST** define at least a :data:`kernel` and
              a list of :data:`attributes`.

    **Example**::

        # TODO 

    .. data:: kernel 

       (`Attribute`) The name of the MD engine / kernel to use (`string`) [`mandatory`].

    .. data:: min_version 

       (`Attribute`) The minimum MD engine / kernel version required (`string`) [`optional`].

    .. data:: arguments 

       (`Attribute`) The arguments to pass to :data:`kernel` (`list` of `strings`) [`mandatory`].

    .. data:: input_data 

       (`Attribute`) The input files that need to be transferred before execution (`transfer directive string`) [`optional`].

       .. note:: TODO: Explain transfer directives.

    .. data:: output_data 

       (`Attribute`) The output files that need to be transferred back after execution (`transfer directive string`) [`optional`].

       .. note:: TODO: Explain transfer directives.

    """
    def __init__(self):
        """Le constructeur.
        """ 

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        self._attributes_register(KERNEL,                None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(ARGUMENTS,             None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(MIN_VERSION,           None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(INPUT_DATA,            None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(OUTPUT_DATA,           None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(COPY_LOCAL_INPUT_DATA, None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(PURGE,                 None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)

    def bind(self, resource):
        """Binds a class:`radical.ensemblemd.mdkernels.MDTaskDescription` to a 
           specific resource. The resulting class:`radical.ensemblemd.mdkernels.BoundMDTask`
           contains the description of executables and arguments necessary to 
           execute the MDTask on the specified resource.
        """

        try: 
          kernel = kerneldict[self.kernel][resource]

          if 'uses_mpi' in kernel:
              uses_mpi = bool(kernel['uses_mpi']),
          else:
              uses_mpi = False

          if 'environment' in kernel:
              environment = kernel['environment']
          else:
              environment = None

          pre_exec = []
          post_exec = []

          # Process pre-execution stuff          
          if 'pre_exec' in kernel and kernel['pre_exec'] is not None:
            pre_exec.extend(kernel['pre_exec'])

          # Process local input file copy as part of pre-execution
          if self.copy_local_input_data is not None:
              for lid in self.copy_local_input_data:
                  pre_exec.append("cp {0} .".format(lid))

          if self.purge is True:
              post_exec.append("rm -rf *")

          bmds = BoundMDTask(
              _environment = environment,
              _pre_exec    = pre_exec,
              _post_exec   = post_exec,
              _executable  = kernel['executable'], 
              _mpi         = uses_mpi,
              _arguments   = self.arguments, 
              _resource    = resource, 
              _input_data  = self.input_data, 
              _output_data = self.output_data
          )

          return bmds

        except Exception, ex:
          logger.error("Couldn't bind MDTaskDescription to resource '{0}': {1}".format(resource, str(ex)))
          raise ex

