.. _char_count:


****************************************
Character Count Application
****************************************

We have, in fact, covered all the features of Ensemble MD toolkit and properties of the pipeline pattern. We will use this experience to create a complete example in this section. We will create a character count application which has 3 steps: file creation, character count, checksum. 

You can download the entire script for this section :download:`here <examples/ccount.py>`.

In the first step, we create a file using the "misc.mkfile" kernel. The second step uses the output of the first step and counts the number of characters in the file. Finally, the third step uses the output file of the second step and performs a checksum operation.


To run the script, simply execute the following from command line:

::

     RADICAL_ENMD_VERBOSE=REPORT python ccount.py
     

A look at the complete code in this section:

.. literalinclude:: examples/ccount.py
