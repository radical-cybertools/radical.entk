.. _writing_kernels:


Writing New Application Kernels
-------------------------------

While the current set of available application kernels might provide a good set of tools to start, sooner or later you will probably want to use a tool for which no application Kernel exsits. This section describes how you can add your custom kernels. 

We have two files, user_script.py which contains the user application which **uses** our custom kernel, new_kernel.py which contains the definition of the custom kernel. You can download them from the following links:

* :download:`user_script.py <../scripts/user_script.py>`
* :download:`new_kernel.py <../scripts/new_kernel.py>`


Let's first take a look at ``new_kernel.py``.

.. literalinclude:: ../scripts/new_kernel.py
        :language: python
        :linenos:

Lines 5-24 contain information about the kernel to be defined. "name" and "arguments" keys are mandatory. The "arguments" key needs to specify the arguments the kernel expects. You can specify whether the individual arguments are mandatory or not. "machine_configs" is not mandatory, but creating a dictionary with resource names (same as defined in the SingleClusterEnvironment) as keys and values which are resource specific lets use the same kernel to be used on different machines.

In lines 28-50, we define a user defined class (of "KernelBase" type) with two mandatory functions. First the constructor, self-explanatory. Second, ``_bind_to_resource`` which is the function that (as the name suggests) binds the kernel with its resource specific values, during execution. In lines 41, 43-45, you can see how the "machine_configs" dictionary approach is helping us solve the tool-level heterogeneity across resources. There might be other ways to do this (if conditions,etc), but we feel this could be quite convenient.

Now, let's take a look at ``user_script.py``

.. literalinclude:: ../scripts/user_script.py
        :language: python
        :linenos:

There are 3 important lines in this script. In line 7, we import the get_engine function in order to register our new kernel. In line 10, we import our new kernel and in line 13, we register our kernel. **THAT'S IT**. We can continue with the application as in the previous examples.