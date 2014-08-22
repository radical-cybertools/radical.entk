A Pipeline of Ensembles
-----------------------


This example shows how to use EnsembleMD Toolkit to execute a simple 
pipeline of sequential ensembles. In the first step 'pre', 16 10 MB input 
files are generated and filled with ASCII charaters. In the second step 
'proc', a character frequency analysis if performed on these file. In the 
last step 'post', an SHA1 checksum is calculated for each analysis result.

The results of the frequency analysis and the SHA1 checksums are copied
back to the machine on which this script executes. 

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python pipeline_of_ensembles.py

:download:`Download example: pipeline_of_ensembles.py <../examples/pipeline_of_ensembles.py>`

.. literalinclude:: ../examples/pipeline_of_ensembles.py

A Pipeline of Tasks
-------------------

 
This example shows how to use EnsembleMD Toolkit to execute a simple 
pipeline of sequential tasks. In the first step ``pre``, a 10 MB input file
is generated and filled with ASCII charaters. In the second step ``proc``,
a character frequency analysis if performed on this file. In the last step
``post``, an SHA1 checksum is calculated for the analysis result.

The results of the frequency analysis and the SHA1 checksums are copied
back to the machine on which this script executes. 

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python pipeline_of_tasks.py

:download:`Download example: pipeline_of_tasks.py <../examples/pipeline_of_tasks.py>`

.. literalinclude:: ../examples/pipeline_of_tasks.py

A Single Ensemble
--------------


This example shows how to use EnsembleMD Toolkit to execute a single 
ensemble of tasks.

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see  log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python single_ensemble.py

:download:`Download example: single_ensemble.py <../examples/single_ensemble.py>`

.. literalinclude:: ../examples/single_ensemble.py

