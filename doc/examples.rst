batch-pipeline.py
-----------------


This example shows how to use EnsembleMD Toolkit to execute a simple 
pipeline of sequential batches. In the first step 'pre', 16 10 MB input 
files are generated and filled with ASCII charaters. In the second step 
'proc', a character frequency analysis if performed on these file. In the 
last step 'post', an SHA1 checksum is calculated for each analysis result.

The results of the frequency analysis and the SHA1 checksums are copied
back to the machine on which this script executes. 

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python batch_pipeline.py

:download:`Download example: batch-pipeline.py <../examples/batch-pipeline.py>`

.. literalinclude:: ../examples/batch-pipeline.py

simple-pipeline.py
------------------

 
This example shows how to use EnsembleMD Toolkit to execute a simple 
pipeline of sequential tasks. In the first step ``pre``, a 10 MB input file
is generated and filled with ASCII charaters. In the second step ``proc``,
a character frequency analysis if performed on this file. In the last step
``post``, an SHA1 checksum is calculated for the analysis result.

The results of the frequency analysis and the SHA1 checksums are copied
back to the machine on which this script executes. 

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python simple-pipeline.py

:download:`Download example: simple-pipeline.py <../examples/simple-pipeline.py>`

.. literalinclude:: ../examples/simple-pipeline.py

single-batch.py
---------------


This example shows how to use EnsembleMD Toolkit to execute a single 
batch of tasks.

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see  log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python single-batch.py

:download:`Download example: single-batch.py <../examples/single-batch.py>`

.. literalinclude:: ../examples/single-batch.py

