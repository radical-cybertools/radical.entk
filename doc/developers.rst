Writing a New Execution Plug-In
-------------------------------

These instructions assume that the execution pattern that we want to write 
a new plug-in for already exists. The two possible execution plug-in 
development scenarios are (1) no execution plug-in exists for the pattern and 
(2) an execution plug-in for the pattern already exists but we want to 
implement an addition / alternative one. In both cases the development 
trajectories are identical. 

Consider the following code snippet that instantiates an execution pattern and 
passes it to an execution context for execution:

.. code-block:: python

    from radical.ensemblemd import EnsemblemdError
    from radical.ensemblemd import StaticExecutionContext
    from radical.ensemblemd import SimulationAnalysisPattern

    try:
        sec = StaticExecutionContext()
        pat = MyPattern()
        sec.execute(pat)

    except EnsemblemdError, er:
        print "EnsembleMD Error: {0}".format(str(er))

If we run this example and no execution plug-in exists for ``MyPattern`` yet, 
we will end up with the following error::

    2014:08:12 14:28:42 36216  MainThread   radical.ensemblemd.Engine: [INFO    ] Loaded execution context plugin 'dummy.static.default' from radical.ensemblemd.execplugins.dummy.static
    2014:08:12 14:28:42 36216  MainThread   radical.ensemblemd.Engine: [INFO    ] Loaded execution context plugin 'dummy.dynamic.default' from radical.ensemblemd.execplugins.dummy.dynamic
    2014:08:12 14:28:42 36216  MainThread   radical.ensemblemd.Engine: [ERROR   ] Couldn't find an execution plug-in for pattern 'MyPattern' and execution context 'Static'.
    EnsembleMD Error: Couldn't find an execution plug-in for pattern 'MyPattern' and execution context 'Static'.

If a plug-in for the pattern / context combination already exists, we can pass
the optional ``force_plugin`` flag to the :func:`radical.ensemblemd.ExecutionContext.execute`
method to force-load a specific plug-in. In this case, use the name we have in mind 
for your new plug-in:

.. code-block:: python

    sec.execute(pat, force_plugin="mypattern.static.improved")

With this minimalist test-rig in place, we can now go ahead and implement the
plug-in. 

First, we create a new file in the ``src/radical/ensemblemd/execplugins`` directory.
The filename is in principle irrelevant, but for practical purposes it should 
obviously help to identify the plug-in. We can even organize things in a sub-directory
if we want. The only requirement is that this directory contains an empty ``__init__.py file.``
For this example, we create the following structure::

    src/radical/ensemblemd/execplugins/
    |
    |----> mypattern/
    |      |
    |      |----> __init__.py
    |      |----> static_improved.py
    |

The module ``static_improved.py`` will contain our new plug-in implementation.
In order for the plug-in to get loaded when EnMD starts up, we need to add it 
to the plug-in registry in ``src/radical/ensemblemd/engine/plugin_registry.py``:

.. code-block:: python

    plugin_registry = [ ...
                        ...
                       "radical.ensemblemd.execplugins.mypattern.static_improved"
                      ]

If we now install the current source branch again (``easy_install .``) and run 
our test-rig again (with ``RADICAL_ENSEMBLEMD_VERBOSE=info`` set in the 
environment), we should see the following error::

    [...]
    2014:08:12 19:29:58 41576  MainThread   radical.ensemblemd.Engine: [ERROR   ] Skipping execution context plugin radical.ensemblemd.execplugins.mypattern.static_improved: loading failed: ''module' object has no attribute 'Adaptor''

    2014:08:12 19:29:58 41576  MainThread   radical.ensemblemd.Engine: [ERROR   ] Couldn't find an execution plug-in for pattern 'MyPattern' and execution context 'Static'.
    EnsembleMD Error: Couldn't find an execution plug-in for pattern 'MyPattern' and execution context 'Static'.

The error message **''module' object has no attribute 'Adaptor''** is expected 
since our plug-in is at this point just an empty file without an implementation.
Let's work on this.


