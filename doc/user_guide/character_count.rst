.. _char_count:


****************************************
Character Count Application
****************************************

We have, in fact, covered all the features of Ensemble toolkit and properties of the pipeline pattern. We will use this experience to create a complete example in this section. We will create a character count application which has 3 stages: file creation, character count, checksum. 

You can download the entire script for this section :download:`here <scripts/ccount.py>` or find it in 
your virtualenv under ``share/radical.ensemblemd/user_guide/scripts``.

In the first stage, we create a file using the "misc.mkfile" kernel. The second stage uses the output of the first stage and counts the number of characters in the file. Finally, the third stage uses the output file of the second stage and performs a checksum operation.


To run the script, simply execute the following from command line:

::

     RADICAL_ENTK_VERBOSE=REPORT python ccount.py
     

You can generate a more verbose output by setting ``RADICAL_ENTK_VERBOSE=INFO``.

A look at the complete code in this section:

.. literalinclude:: scripts/ccount.py
