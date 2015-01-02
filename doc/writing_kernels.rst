.. _dev:

***********************
Developer Documentation
***********************





Writing New Application Kernels
*******************************

While the current set of available application kernels might provide a good
set of tools to start, sooner or later you will probably want to use a tool
for which no application Kernel exsits. In this case, you will have to write
your own one.

The easiest way to describe how this works is by example:

.. literalinclude:: ../examples/user_kernel.py
   :linenos:
   :start-after: __example_name__
